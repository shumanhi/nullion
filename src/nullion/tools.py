"""Typed tool registry for UI-neutral Nullion capabilities."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from email.message import EmailMessage
from enum import Enum
from fnmatch import fnmatch
from html import unescape
from ipaddress import ip_address
import inspect
import json
import logging
import mimetypes
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
from time import perf_counter
from typing import Callable, Iterable, Protocol
import base64
import urllib.error
import urllib.request
from urllib.parse import quote, urlencode, urlparse

from nullion.attachment_format_graph import VALID_ATTACHMENT_EXTENSIONS
from nullion.artifacts import promote_supporting_asset_artifact_paths
from nullion.approval_context import FLOW_TRIGGER_CONTEXT_KEY, build_trigger_flow_context
from nullion.approvals import (
    ApprovalRequest,
    consume_boundary_permit as consume_boundary_permit_record,
    create_approval_request,
    is_boundary_permit_active,
    is_permission_grant_active,
    revoke_permission_grant as revoke_permission_grant_record,
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
from nullion.skill_pack_catalog import normalize_enabled_skill_pack_ids
from nullion.tips import MEDIA_PROVIDER_SETUP_TIP, format_setup_tip
from nullion.tool_boundaries import extract_boundary_facts


_WEB_FETCH_MAX_REDIRECTS = 5
_WEB_FETCH_MAX_BODY_BYTES = 2_000_000
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
    "presentation_create": ("output_path",),
    "spreadsheet_create": ("output_path",),
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
    capability_tags: tuple[str, ...] = ()


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


def _clear_deep_agent_profile_cache() -> None:
    try:
        from nullion.deep_agent_profiles import clear_deep_agent_profile_caches

        clear_deep_agent_profile_caches()
        from nullion.cron_delivery import clear_cron_execution_metadata_caches

        clear_cron_execution_metadata_caches()
    except Exception:
        logger.debug("Could not clear Deep Agents profile cache", exc_info=True)


_TEXT_FILE_WRITE_BLOCKED_EXTENSIONS = frozenset({
    ".doc",
    ".docx",
    ".pdf",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
})
_TERMINAL_DELIVERABLE_ARTIFACT_EXTENSIONS = frozenset(VALID_ATTACHMENT_EXTENSIONS) | _TEXT_FILE_WRITE_BLOCKED_EXTENSIONS
_MAX_TERMINAL_DISCOVERED_ARTIFACTS = 25
_MAX_TERMINAL_ARTIFACT_SCAN_ENTRIES = 10000

_CRON_TOOL_PROPERTIES: dict[str, dict[str, str]] = {
    "id": {"type": "string", "description": "Cron job id. Required for update operations."},
    "name": {"type": "string", "description": "Human-readable cron name."},
    "schedule": {"type": "string", "description": "Cron schedule expression."},
    "task": {"type": "string", "description": "Task instructions to run when the cron fires."},
    "enabled": {"type": "boolean", "description": "Whether the cron is active."},
    "workspace_id": {"type": "string", "description": "Workspace id that owns the cron."},
    "delivery_channel": {"type": "string", "description": "Delivery channel such as web, telegram, slack, or discord."},
    "delivery_target": {"type": "string", "description": "Channel-specific delivery target."},
}


def _cron_tool_properties() -> dict[str, object]:
    return {name: dict(schema) for name, schema in _CRON_TOOL_PROPERTIES.items()}


def _default_input_schema_for_tool(tool_name: str) -> dict[str, object]:
    def cron_tool_properties() -> dict[str, object]:
        return {name: dict(schema) for name, schema in _CRON_TOOL_PROPERTIES.items()}

    schemas: dict[str, dict[str, object]] = {
        "create_cron": {
            "type": "object",
            "properties": cron_tool_properties(),
            "required": ["name", "schedule", "task"],
            "additionalProperties": False,
        },
        "list_crons": {
            "type": "object",
            "properties": {
                "workspace_id": {"type": "string", "description": "Workspace id to list. Defaults to the current workspace."},
                "include_all_workspaces": {"type": "boolean", "description": "Whether to include crons from every workspace."},
            },
            "additionalProperties": False,
        },
        "update_cron": {
            "type": "object",
            "properties": cron_tool_properties(),
            "required": ["id"],
            "additionalProperties": False,
        },
        "delete_cron": {
            "type": "object",
            "properties": {"id": {"type": "string", "description": "Cron job id to delete."}},
            "required": ["id"],
            "additionalProperties": False,
        },
        "toggle_cron": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "Cron job id to enable or disable."},
                "enabled": {"type": "boolean", "description": "True to enable the cron, false to disable it."},
            },
            "required": ["id", "enabled"],
            "additionalProperties": False,
        },
        "run_cron": {
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "Cron job id to run immediately."},
                "name": {"type": "string", "description": "Cron job name to run immediately when id is unavailable."},
            },
            "additionalProperties": False,
        },
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
        "spreadsheet_create": {
            "type": "object",
            "properties": {
                "output_path": {
                    "type": "string",
                    "description": "Optional destination .xlsx path. If omitted, Nullion creates one in the artifact directory.",
                },
                "title": {"type": "string", "description": "Optional workbook title used for the default filename."},
                "sheet_name": {"type": "string", "description": "Optional worksheet name."},
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional ordered column names. If omitted, object row keys are used.",
                },
                "rows": {
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {"type": "object"},
                            {
                                "type": "array",
                                "items": {
                                    "anyOf": [
                                        {"type": "string"},
                                        {"type": "number"},
                                        {"type": "integer"},
                                        {"type": "boolean"},
                                        {"type": "null"},
                                        {"type": "object"},
                                        {"type": "array", "items": {}},
                                    ],
                                },
                            },
                        ],
                    },
                    "description": "Rows as objects keyed by column name, or arrays matching the columns.",
                },
                "image_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional existing image artifact paths to embed, aligned to rows when possible.",
                },
            },
            "additionalProperties": False,
        },
        "presentation_create": {
            "type": "object",
            "properties": {
                "output_path": {
                    "type": "string",
                    "description": "Optional destination .pptx path. If omitted, Nullion creates one in the artifact directory.",
                },
                "title": {"type": "string", "description": "Optional deck title used for the default filename."},
                "slides": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string", "description": "Slide title."},
                            "body": {"type": "string", "description": "Short body text for the slide."},
                            "bullets": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional bullet text for the slide.",
                            },
                            "image_paths": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional existing image artifact paths for this slide.",
                            },
                        },
                        "additionalProperties": False,
                    },
                    "description": "Structured slide contents. If omitted, image_paths are placed one per slide.",
                },
                "image_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional existing image artifact paths to place into slides.",
                },
            },
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
                "shell": {
                    "type": "string",
                    "enum": ["auto", "sh", "bash", "zsh", "powershell", "pwsh", "cmd"],
                    "description": "Optional shell family for this command. Defaults to the platform shell.",
                },
                "network_mode": {
                    "type": "string",
                    "enum": sorted(_VALID_NETWORK_MODES),
                    "description": "Optional network policy for the command.",
                },
                "timeout_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 300,
                    "description": "Optional execution timeout in seconds.",
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
        "weather_forecast": {
            "type": "object",
            "properties": {
                "location_text": {
                    "type": "string",
                    "description": "Optional place name, address, city, or postal code to resolve before fetching forecast data.",
                },
                "latitude": {"type": "number", "description": "Optional decimal latitude."},
                "longitude": {"type": "number", "description": "Optional decimal longitude."},
                "forecast_days": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 7,
                    "description": "Number of forecast days to return. Defaults to 3.",
                },
                "timezone": {
                    "type": "string",
                    "description": "Optional IANA timezone. Defaults to auto from Open-Meteo.",
                },
            },
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
                    "description": (
                        "Full HTTP(S) gateway URL from the installed connector skill, under that "
                        "provider's configured base URL. Do not use generic public web URLs here."
                    ),
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
        "email_send": {
            "type": "object",
            "properties": {
                "to": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Recipient email address(es).",
                },
                "subject": {"type": "string", "description": "Email subject."},
                "body": {"type": "string", "description": "Plain text email body."},
                "cc": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional CC email address(es).",
                },
                "bcc": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional BCC email address(es).",
                },
                "attachment_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional local artifact/media paths to attach.",
                },
                "provider_id": {
                    "type": "string",
                    "description": "Optional connector provider id. Defaults to an active Google Mail connector.",
                },
            },
            "required": ["to", "subject", "body"],
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
                "provider_id": {
                    "type": "string",
                    "description": "Optional connector provider id. Defaults to an active calendar-capable connector.",
                },
            },
            "required": ["start", "end"],
            "additionalProperties": False,
        },
        "browser_navigate": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "HTTP/HTTPS URL, or a local HTML file path/file URL inside this workspace, to open in the browser.",
                }
            },
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
    shell: str = "unknown"
    argv: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TerminalShellInvocation:
    shell: str
    argv: tuple[str, ...]


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
    principal_id: str | None = None,
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
    unknown_workspace_denial = _unknown_workspace_storage_denial(tokens, principal_id=principal_id)
    if unknown_workspace_denial is not None:
        return unknown_workspace_denial

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
    workspace_denial = _terminal_unknown_workspace_denial(tokens, resolved_allowed_roots, principal_id=principal_id)
    if workspace_denial is not None:
        return workspace_denial
    return None


def _unknown_workspace_storage_denial(tokens: list[str], *, principal_id: str | None = None) -> dict[str, object] | None:
    try:
        from nullion.workspace_storage import sanitize_workspace_id, workspace_storage_base

        storage_base = workspace_storage_base()
    except Exception:
        return None
    allowed_workspaces = {
        sanitize_workspace_id(workspace_id)
        for workspace_id in _registered_or_existing_workspace_ids(storage_base)
    }
    for token in tokens:
        if not token or token.startswith("-") or token in {";", "&&", "||", "|"}:
            continue
        try:
            candidate = Path(os.path.expandvars(token)).expanduser().resolve(strict=False)
        except Exception:
            continue
        try:
            relative = candidate.relative_to(storage_base)
        except ValueError:
            continue
        if not relative.parts:
            continue
        workspace_id = sanitize_workspace_id(relative.parts[0])
        if not _workspace_access_allowed(workspace_id, principal_id=principal_id, allowed_workspaces=allowed_workspaces):
            return {
                "reason": "unknown_workspace_denied",
                "workspace_id": workspace_id,
                "path": str(candidate),
                "message": f"Refusing to create or modify unknown workspace storage: {workspace_id}.",
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


def _terminal_path_candidates(token: str) -> tuple[Path, ...]:
    stripped = str(token or "").strip().strip("'\"`")
    if not stripped:
        return ()
    candidates: list[str] = [stripped]
    for separator in ("=", ":"):
        if separator in stripped:
            tail = stripped.split(separator, 1)[1].strip()
            if tail:
                candidates.append(tail)
    paths: list[Path] = []
    for candidate in candidates:
        cleaned = candidate.strip("'\"`")
        if not cleaned:
            continue
        if cleaned.startswith(("~", "/", "$HOME", "${HOME}")) or "/.nullion/workspaces/" in cleaned:
            try:
                paths.append(Path(os.path.expandvars(cleaned)).expanduser().resolve(strict=False))
            except Exception:
                continue
    return tuple(dict.fromkeys(paths))


def _registered_or_existing_workspace_ids(base: Path) -> set[str]:
    ids: set[str] = set()
    try:
        from nullion.users import registered_workspace_ids

        ids.update(registered_workspace_ids())
    except Exception:
        ids.add("workspace_admin")
    try:
        if base.is_dir():
            ids.update(path.name for path in base.iterdir() if path.is_dir())
    except OSError:
        pass
    ids.add("workspace_admin")
    return ids


def _workspace_access_allowed(
    workspace_id: str,
    *,
    principal_id: str | None,
    allowed_workspaces: set[str],
) -> bool:
    try:
        from nullion.connections import workspace_id_for_principal
        from nullion.workspace_storage import sanitize_workspace_id

        principal_workspace = sanitize_workspace_id(workspace_id_for_principal(principal_id))
        target_workspace = sanitize_workspace_id(workspace_id)
    except Exception:
        principal_workspace = "workspace_admin"
        target_workspace = str(workspace_id or "workspace_admin")
    if principal_workspace == target_workspace:
        return True
    if principal_workspace == "workspace_admin" and target_workspace in allowed_workspaces:
        return True
    return False


def _terminal_unknown_workspace_denial(
    tokens: list[str],
    allowed_roots: Iterable[Path],
    *,
    principal_id: str | None = None,
) -> dict[str, object] | None:
    _ = allowed_roots
    try:
        from nullion.workspace_storage import workspace_storage_base

        base = workspace_storage_base()
    except Exception:
        return None
    allowed = _registered_or_existing_workspace_ids(base)
    for token in tokens:
        for path in _terminal_path_candidates(token):
            try:
                relative = path.relative_to(base)
            except ValueError:
                continue
            if not relative.parts:
                continue
            workspace_id = relative.parts[0]
            if _workspace_access_allowed(workspace_id, principal_id=principal_id, allowed_workspaces=allowed):
                continue
            return {
                "reason": "unknown_workspace_denied",
                "workspace_id": workspace_id,
                "path": str(path),
                "message": (
                    "Refusing to use or create an unregistered workspace directory. "
                    "Verify the person/workspace exists in the Users settings first."
                ),
            }
    return None


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
        shell: str | None = None,
    ) -> TerminalExecutionResult: ...


_POSIX_SHELL_CHOICES = {
    "sh": ("sh", "-lc"),
    "bash": ("bash", "-lc"),
    "zsh": ("zsh", "-lc"),
}
_WINDOWS_SHELL_CHOICES = {
    "pwsh": ("pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command"),
    "powershell": ("powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command"),
    "cmd": ("cmd.exe", "/d", "/s", "/c"),
}


def _normalize_terminal_shell(raw_shell: object) -> str | None:
    if not isinstance(raw_shell, str):
        return None
    shell = raw_shell.strip().lower()
    return shell or None


def _terminal_shell_from_env() -> str | None:
    raw_shell = os.environ.get("NULLION_TERMINAL_SHELL")
    if not raw_shell:
        return None
    shell = raw_shell.strip()
    if not shell:
        return None
    shell_name = Path(shell).name.lower()
    if shell_name in {"pwsh", "pwsh.exe"}:
        return "pwsh"
    if shell_name in {"powershell", "powershell.exe"}:
        return "powershell"
    if shell_name in {"cmd", "cmd.exe"}:
        return "cmd"
    if shell_name in _POSIX_SHELL_CHOICES:
        return shell_name
    return shell


def _which_shell(executable: str) -> str | None:
    if Path(executable).parent != Path():
        return executable if Path(executable).exists() else None
    return shutil.which(executable)


def _resolve_posix_terminal_shell(command: str, requested_shell: str | None) -> TerminalShellInvocation:
    shell = requested_shell if requested_shell in _POSIX_SHELL_CHOICES else None
    if shell is None and requested_shell in _WINDOWS_SHELL_CHOICES:
        shell = "sh"
    if shell is None:
        env_shell = _terminal_shell_from_env()
        if env_shell and env_shell not in _POSIX_SHELL_CHOICES:
            resolved_env_shell = _which_shell(env_shell)
            if resolved_env_shell is not None:
                return TerminalShellInvocation(
                    shell=Path(resolved_env_shell).name.lower(),
                    argv=(resolved_env_shell, "-lc", command),
                )
        shell = env_shell if env_shell in _POSIX_SHELL_CHOICES else "sh"
    executable, flag = _POSIX_SHELL_CHOICES.get(shell, ("sh", "-lc"))
    resolved = _which_shell(executable) or "/bin/sh"
    return TerminalShellInvocation(shell=shell, argv=(resolved, flag, command))


def _resolve_windows_terminal_shell(command: str, requested_shell: str | None) -> TerminalShellInvocation:
    candidates: list[str] = []
    if requested_shell and requested_shell != "auto":
        candidates.append(requested_shell)
    env_shell = _terminal_shell_from_env()
    if env_shell:
        candidates.append(env_shell)
    candidates.extend(("pwsh", "powershell", "cmd"))

    seen: set[str] = set()
    for shell in candidates:
        if shell in seen:
            continue
        seen.add(shell)
        template = _WINDOWS_SHELL_CHOICES.get(shell)
        if template is None:
            continue
        executable = _which_shell(template[0])
        if executable is None:
            continue
        return TerminalShellInvocation(shell=shell, argv=(executable, *template[1:], command))

    comspec = os.environ.get("COMSPEC")
    cmd = comspec if comspec else "cmd.exe"
    return TerminalShellInvocation(shell="cmd", argv=(cmd, "/d", "/s", "/c", command))


def _resolve_terminal_shell_invocation(command: str, *, shell: str | None = None) -> TerminalShellInvocation:
    requested_shell = _normalize_terminal_shell(shell) or "auto"
    if os.name == "nt":
        return _resolve_windows_terminal_shell(command, requested_shell)
    return _resolve_posix_terminal_shell(command, requested_shell)


def _terminal_timeout_seconds(raw_timeout: object, *, default: int = 20, maximum: int = 300) -> int:
    if isinstance(raw_timeout, bool):
        return default
    try:
        timeout = int(raw_timeout) if raw_timeout is not None else default
    except (TypeError, ValueError):
        return default
    return max(1, min(maximum, timeout))


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
        shell: str | None = None,
    ) -> TerminalExecutionResult:
        del policy
        invocation = _resolve_terminal_shell_invocation(command, shell=shell)
        try:
            completed = subprocess.run(
                list(invocation.argv),
                shell=False,
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
                shell=invocation.shell,
                argv=invocation.argv,
            )
        except OSError as exc:
            return TerminalExecutionResult(
                exit_code=127,
                stdout="",
                stderr=f"Shell startup failed: {exc}",
                shell=invocation.shell,
                argv=invocation.argv,
            )
        return TerminalExecutionResult(
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            shell=invocation.shell,
            argv=invocation.argv,
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
        shell: str | None = None,
    ) -> TerminalExecutionResult:
        del shell
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
                shell="launcher",
                argv=invocation.argv,
            )
        except OSError as exc:
            return TerminalExecutionResult(
                exit_code=127,
                stdout="",
                stderr=f"Terminal launcher startup failed: {exc}",
                shell="launcher",
                argv=invocation.argv,
            )
        return TerminalExecutionResult(
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            shell="launcher",
            argv=invocation.argv,
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
        _clear_deep_agent_profile_cache()

    def unregister(self, name: str) -> None:
        self._specs.pop(name, None)
        self._handlers.pop(name, None)
        _clear_deep_agent_profile_cache()

    def get_spec(self, name: str) -> ToolSpec:
        return self._specs[name]

    def list_specs(self) -> list[ToolSpec]:
        return [self._specs[name] for name in sorted(self._specs)]

    def list_tool_definitions(self) -> list[dict[str, object]]:
        return [
            {
                "name": spec.name,
                "description": spec.description,
                "input_schema": getattr(spec, "input_schema", None)
                or _default_input_schema_for_tool(spec.name),
                "capability_tags": list(getattr(spec, "capability_tags", ()) or ()),
                "side_effect_class": str(
                    getattr(spec.side_effect_class, "value", spec.side_effect_class)
                ),
                "risk_level": str(getattr(spec.risk_level, "value", spec.risk_level)),
                "requires_approval": bool(spec.requires_approval),
            }
            for spec in self.list_specs()
        ]

    def filesystem_allowed_roots(self) -> tuple[Path, ...]:
        return self._filesystem_allowed_roots

    def set_filesystem_allowed_roots(self, roots: Iterable[Path]) -> None:
        self._filesystem_allowed_roots = tuple(Path(root).resolve() for root in roots)

    def mark_plugin_installed(self, plugin_name: str) -> None:
        normalized = plugin_name.strip()
        if normalized:
            self._installed_plugins.add(normalized)

    def unmark_plugin_installed(self, plugin_name: str) -> None:
        normalized = plugin_name.strip()
        if normalized:
            self._installed_plugins.discard(normalized)

    def list_installed_plugins(self) -> list[str]:
        return sorted(self._installed_plugins)

    def is_plugin_installed(self, plugin_name: str) -> bool:
        return plugin_name in self._installed_plugins

    @staticmethod
    def _missing_required_schema_arguments(schema: dict[str, object], arguments: dict[str, object]) -> list[str]:
        missing: list[str] = []
        for raw_name in schema.get("required", ()) or ():
            name = str(raw_name)
            value = arguments.get(name)
            if value is None or (isinstance(value, str) and not value.strip()):
                missing.append(name)
        return missing

    def _preflight_schema_result(self, spec: ToolSpec, invocation: ToolInvocation) -> ToolResult | None:
        schema = spec.input_schema or _default_input_schema_for_tool(spec.name)
        if not isinstance(schema, dict):
            return None
        missing = self._missing_required_schema_arguments(schema, invocation.arguments or {})
        if not missing:
            return None
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="failed",
            output={
                "reason": "invalid_tool_arguments",
                "missing_required": missing,
                "suppress_activity": True,
            },
            error=f"Missing required arguments: {', '.join(missing)}",
        )

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
        handler = self._handlers.get(invocation.tool_name)
        if handler is None:
            raise KeyError(f"Unknown tool: {invocation.tool_name}")
        spec = self._specs[invocation.tool_name]
        preflight = self._preflight_schema_result(spec, invocation)
        if preflight is not None:
            return preflight
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


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _connector_access_enabled() -> bool:
    raw = os.environ.get("NULLION_CONNECTOR_ACCESS_ENABLED")
    if raw is not None and raw.strip():
        return _env_flag("NULLION_CONNECTOR_ACCESS_ENABLED")
    enabled_packs = normalize_enabled_skill_pack_ids(
        [
            item
            for item in str(os.environ.get("NULLION_ENABLED_SKILL_PACKS") or "").split(",")
            if item.strip()
        ]
    )
    if "nullion/connector-skills" in enabled_packs or "maton-ai/api-gateway-skill" in enabled_packs:
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
    provider_id = str(invocation.arguments.get("provider_id") or "").strip()
    if not provider_id:
        if invocation.tool_name in {"email_search", "email_read"}:
            try:
                from nullion.connections import default_email_connector_provider_id

                provider_id = default_email_connector_provider_id(invocation.principal_id) or ""
            except Exception:
                provider_id = ""
        if not provider_id:
            return False
    try:
        from nullion.connections import connection_for_principal

        connection = connection_for_principal(invocation.principal_id, provider_id)
    except Exception:
        connection = None
    if connection is None or not getattr(connection, "active", True):
        return False
    if invocation.tool_name in {"email_search", "email_read"}:
        return (
            fact.kind is BoundaryKind.ACCOUNT_ACCESS
            and fact.operation == "read"
            and fact.target == provider_id
        )
    if invocation.tool_name != "connector_request":
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
        if invocation.tool_name == "email_send":
            return bool(self._matching_email_send_review_grants(invocation))
        return self._has_active_grant(
            principal_id=invocation.principal_id,
            permissions=(
                f"tool:{invocation.tool_name}",
                f"tool.{invocation.tool_name}",
                invocation.tool_name,
            ),
        )

    def _matching_email_send_review_grants(self, invocation: ToolInvocation):
        current_arguments = redact_value(dict(invocation.arguments or {}))
        matching_approval_ids: set[str] = set()
        for approval in self._store.list_approval_requests():
            if getattr(getattr(approval, "status", None), "value", getattr(approval, "status", "")) != "approved":
                continue
            if approval.requested_by != invocation.principal_id:
                continue
            if approval.action != "use_tool" or approval.resource != "email_send":
                continue
            context = approval.context if isinstance(approval.context, dict) else {}
            if context.get("tool_arguments") == current_arguments:
                matching_approval_ids.add(approval.approval_id)
        if not matching_approval_ids:
            return []
        grants = []
        principal_ids = {
            invocation.principal_id,
            permission_scope_principal(invocation.principal_id),
            OPERATOR_PERMISSION_PRINCIPAL,
        }
        for grant in self._store.list_permission_grants():
            if grant.approval_id not in matching_approval_ids:
                continue
            if grant.principal_id not in principal_ids:
                continue
            if grant.permission not in {"tool:email_send", "tool.email_send", "email_send"}:
                continue
            if is_permission_grant_active(grant):
                grants.append(grant)
        return grants

    def _revoke_email_send_review_grants(self, invocation: ToolInvocation) -> None:
        for grant in self._matching_email_send_review_grants(invocation):
            try:
                self._store.add_permission_grant(
                    revoke_permission_grant_record(
                        grant,
                        revoked_by="runtime",
                        revoked_at=datetime.now(UTC),
                        reason="Email send review approval consumed.",
                    )
                )
            except Exception:
                logger.debug("Could not revoke consumed email_send approval grant", exc_info=True)

    def _find_pending_tool_approval(self, invocation: ToolInvocation):
        current_arguments = redact_value(dict(invocation.arguments or {})) if invocation.tool_name == "email_send" else None
        for approval in self._store.list_approval_requests():
            if approval.status.value != "pending":
                continue
            if approval.requested_by != invocation.principal_id:
                continue
            if approval.action != "use_tool":
                continue
            if approval.resource != invocation.tool_name:
                continue
            if current_arguments is not None:
                context = approval.context if isinstance(approval.context, dict) else {}
                if context.get("tool_arguments") != current_arguments:
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

        try:
            spec = self._registry.get_spec(invocation.tool_name)
        except KeyError:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"reason": "tool_not_available"},
                error=f"Tool is not available in this turn: {invocation.tool_name}",
            )

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

        started_at = perf_counter()
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
        duration_ms = (perf_counter() - started_at) * 1000
        result = normalize_tool_result(result)
        if invocation.tool_name == "email_send" and result.status != "denied":
            self._revoke_email_send_review_grants(invocation)
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
        try:
            from nullion.builder_routes import build_route_observation

            observation = build_route_observation(
                invocation=invocation,
                result=result,
                duration_ms=duration_ms,
                capability_tags=getattr(spec, "capability_tags", ()) or (),
            )
            if observation is not None:
                add_observation = getattr(self._store, "add_builder_route_observation", None)
                if callable(add_observation):
                    add_observation(observation)
                logger.info(
                    "tool route timing tool=%s source=%s status=%s duration_ms=%.1f reason=%s capsule_id=%s principal_id=%s",
                    observation.get("tool_name"),
                    observation.get("source_domain") or "unknown",
                    observation.get("status"),
                    float(observation.get("duration_ms") or 0.0),
                    observation.get("reason") or "",
                    observation.get("capsule_id") or "",
                    observation.get("principal_id") or "",
                )
        except Exception:
            logger.debug("Builder route observation failed", exc_info=True)
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
        if path.suffix.lower() in _TEXT_FILE_WRITE_BLOCKED_EXTENSIONS:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"path": str(path)},
                error=(
                    f"file_write is text-only and cannot create {path.suffix.lower()} artifacts. "
                    "Use a dedicated artifact tool or verified generator for that file type."
                ),
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


_SPREADSHEET_IMAGE_KEYS = ("image_path", "image_paths", "image")


def _json_scalar_for_spreadsheet(value: object) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _spreadsheet_columns(raw_columns: object, rows: list[object]) -> list[str]:
    columns = [str(column).strip() for column in raw_columns or () if str(column or "").strip()] if isinstance(raw_columns, (list, tuple)) else []
    if columns:
        return list(dict.fromkeys(columns))
    discovered: list[str] = []
    for row in rows:
        if isinstance(row, dict):
            for key in row:
                if key in _SPREADSHEET_IMAGE_KEYS:
                    continue
                column = str(key).strip()
                if column and column not in discovered:
                    discovered.append(column)
        elif isinstance(row, (list, tuple)):
            for index in range(len(row)):
                column = f"Column {index + 1}"
                if column not in discovered:
                    discovered.append(column)
    return discovered or ["Value"]


def _spreadsheet_row_values(row: object, columns: list[str]) -> list[object]:
    if isinstance(row, dict):
        return [_json_scalar_for_spreadsheet(row.get(column)) for column in columns]
    if isinstance(row, (list, tuple)):
        values = [_json_scalar_for_spreadsheet(value) for value in row]
        if len(values) < len(columns):
            values.extend([None] * (len(columns) - len(values)))
        return values[: len(columns)]
    return [_json_scalar_for_spreadsheet(row), *([None] * max(0, len(columns) - 1))]


def _spreadsheet_row_image_path(row: object, fallback: object) -> str | None:
    candidates: list[object] = []
    if isinstance(row, dict):
        for key in _SPREADSHEET_IMAGE_KEYS:
            value = row.get(key)
            if isinstance(value, (list, tuple)):
                candidates.extend(value)
            else:
                candidates.append(value)
    candidates.append(fallback)
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _spreadsheet_output_path(
    invocation: ToolInvocation,
    *,
    raw_path: object,
    title: object,
) -> Path:
    if isinstance(raw_path, str) and raw_path.strip():
        return Path(raw_path).expanduser().resolve()
    from nullion.artifacts import artifact_path_for_generated_workspace_file

    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", str(title or "spreadsheet").strip()).strip("-._")
    return artifact_path_for_generated_workspace_file(
        principal_id=invocation.principal_id,
        suffix=".xlsx",
        stem=stem or "spreadsheet",
    )


def _build_spreadsheet_create_handler(
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
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "File access requires workspace_root or allowed_roots")

        try:
            from openpyxl import Workbook
            from openpyxl.drawing.image import Image as WorksheetImage
            from openpyxl.styles import Font
        except ModuleNotFoundError as exc:
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {
                    "reason": "missing_dependency",
                    "dependency_id": "openpyxl",
                    "dependency": "openpyxl",
                    "package": "openpyxl",
                    "requirement": "openpyxl>=3.1,<4",
                    "license": "MIT",
                    "install_command": "python -m pip install 'openpyxl>=3.1,<4'",
                },
                f"spreadsheet_create requires openpyxl: {exc}",
            )

        rows = list(invocation.arguments.get("rows") or [])
        if not isinstance(rows, list):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "rows must be a list")
        columns = _spreadsheet_columns(invocation.arguments.get("columns"), rows)
        image_paths = list(invocation.arguments.get("image_paths") or [])
        if not isinstance(image_paths, list):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "image_paths must be a list")
        row_image_paths = [
            _spreadsheet_row_image_path(row, image_paths[index] if index < len(image_paths) else None)
            for index, row in enumerate(rows)
        ]
        include_image_column = any(row_image_paths)
        workbook_columns = [*columns, *(["Image"] if include_image_column else [])]
        output_path = _spreadsheet_output_path(
            invocation,
            raw_path=invocation.arguments.get("output_path"),
            title=invocation.arguments.get("title"),
        )
        if output_path.suffix.lower() != ".xlsx":
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {"path": str(output_path)}, "spreadsheet_create output_path must end in .xlsx")
        if not _path_within_any_root(output_path, effective_roots) and not _is_approved_filesystem_path(
            output_path,
            invocation.trusted_filesystem_selectors,
        ):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"Path is outside workspace root: {output_path}")

        wb = Workbook()
        ws = wb.active
        sheet_name = str(invocation.arguments.get("sheet_name") or "Sheet1").strip() or "Sheet1"
        ws.title = re.sub(r"[\[\]:*?/\\]", " ", sheet_name)[:31] or "Sheet1"
        ws.append(workbook_columns)
        for cell in ws[1]:
            cell.font = Font(bold=True)

        embedded_images: list[str] = []
        skipped_images: list[str] = []
        image_column_index = len(workbook_columns) if include_image_column else None
        for row_index, row in enumerate(rows, start=2):
            values = _spreadsheet_row_values(row, columns)
            ws.append([*values, *([""] if include_image_column else [])])
            for col_index, value in enumerate(values, start=1):
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    cell = ws.cell(row=row_index, column=col_index)
                    cell.hyperlink = value
                    cell.font = Font(color="0563C1", underline="single")
            image_path_text = row_image_paths[row_index - 2]
            if image_column_index is None or not image_path_text:
                continue
            image_path = Path(_resolve_virtual_workspace_path(image_path_text, principal_id=invocation.principal_id)).expanduser().resolve()
            if not image_path.is_file() or not _path_within_any_root(image_path, effective_roots):
                skipped_images.append(image_path_text)
                continue
            try:
                image = WorksheetImage(str(image_path))
                if image.width > 140:
                    scale = 140 / float(image.width)
                    image.width = int(image.width * scale)
                    image.height = int(image.height * scale)
                if image.height > 120:
                    scale = 120 / float(image.height)
                    image.width = int(image.width * scale)
                    image.height = int(image.height * scale)
                ws.add_image(image, f"{ws.cell(row=row_index, column=image_column_index).coordinate}")
                ws.row_dimensions[row_index].height = max(ws.row_dimensions[row_index].height or 15, 92)
                embedded_images.append(str(image_path))
            except Exception:
                skipped_images.append(image_path_text)

        for column_cells in ws.columns:
            header = str(column_cells[0].value or "")
            max_length = max(len(str(cell.value or "")) for cell in column_cells[:100])
            ws.column_dimensions[column_cells[0].column_letter].width = min(max(max_length + 2, len(header) + 2, 12), 48)
        if image_column_index is not None:
            ws.column_dimensions[ws.cell(row=1, column=image_column_index).column_letter].width = 22

        output_path.parent.mkdir(parents=True, exist_ok=True)
        wb.save(output_path)
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "path": str(output_path),
                "artifact_path": str(output_path),
                "artifact_paths": [str(output_path)],
                "rows": len(rows),
                "columns": len(workbook_columns),
                "embedded_images": embedded_images,
                "skipped_images": skipped_images,
            },
            None,
        )

    return handler


def _presentation_output_path(
    invocation: ToolInvocation,
    *,
    raw_path: object,
    title: object,
    roots: tuple[Path, ...],
) -> Path:
    if isinstance(raw_path, str) and raw_path.strip():
        return Path(raw_path).expanduser().resolve()
    try:
        from nullion.artifacts import artifact_path_for_generated_workspace_file

        return artifact_path_for_generated_workspace_file(
            principal_id=invocation.principal_id,
            suffix=".pptx",
            stem=_safe_pdf_stem(str(title or "presentation")),
        ).resolve()
    except Exception:
        return (roots[0] / f"{_safe_pdf_stem(str(title or 'presentation'))}.pptx").resolve()


def _presentation_slide_specs(raw_slides: object, image_paths: list[str], *, title: str) -> tuple[list[dict[str, object]], str | None]:
    if raw_slides is not None and not isinstance(raw_slides, list):
        return [], "slides must be a list"
    slides: list[dict[str, object]] = []
    for index, raw_slide in enumerate(raw_slides or [], start=1):
        if not isinstance(raw_slide, dict):
            return [], "slides entries must be objects"
        bullets, bullet_error = _coerce_string_list(raw_slide.get("bullets"), field=f"slides[{index}].bullets")
        if bullet_error is not None:
            return [], bullet_error
        slide_images, image_error = _coerce_string_list(raw_slide.get("image_paths"), field=f"slides[{index}].image_paths")
        if image_error is not None:
            return [], image_error
        slides.append(
            {
                "title": str(raw_slide.get("title") or f"Slide {index}").strip() or f"Slide {index}",
                "body": str(raw_slide.get("body") or "").strip(),
                "bullets": bullets,
                "image_paths": slide_images,
            }
        )
    if not slides:
        if image_paths:
            slides = [
                {"title": title or f"Image {index}", "body": "", "bullets": [], "image_paths": [image_path]}
                for index, image_path in enumerate(image_paths, start=1)
            ]
        else:
            slides = [{"title": title or "Presentation", "body": "", "bullets": [], "image_paths": []}]
    elif image_paths:
        for index, image_path in enumerate(image_paths):
            target = slides[index] if index < len(slides) else slides[-1]
            target.setdefault("image_paths", [])
            cast_images = target["image_paths"]
            if isinstance(cast_images, list):
                cast_images.append(image_path)
    return slides, None


def _resolve_presentation_image_paths(
    raw_paths: list[str],
    *,
    roots: tuple[Path, ...],
    invocation: ToolInvocation,
) -> tuple[list[Path], list[str], str | None]:
    resolved: list[Path] = []
    skipped: list[str] = []
    for raw_path in raw_paths:
        image_path = Path(_resolve_virtual_workspace_path(raw_path, principal_id=invocation.principal_id)).expanduser().resolve()
        if not _path_within_any_root(image_path, roots) and not _is_approved_filesystem_path(
            image_path,
            invocation.trusted_filesystem_selectors,
        ):
            return resolved, skipped, f"Image path is outside workspace root: {image_path}"
        if not image_path.is_file():
            skipped.append(raw_path)
            continue
        resolved.append(image_path)
    return resolved, skipped, None


def _add_presentation_text(slide, *, title: str, body: str, bullets: list[str], has_images: bool) -> None:
    from pptx.util import Inches, Pt

    title_box = slide.shapes.add_textbox(Inches(0.45), Inches(0.25), Inches(9.1), Inches(0.55))
    title_frame = title_box.text_frame
    title_frame.clear()
    paragraph = title_frame.paragraphs[0]
    paragraph.text = title[:120]
    paragraph.font.bold = True
    paragraph.font.size = Pt(28)

    text_width = Inches(4.25 if has_images else 9.1)
    body_box = slide.shapes.add_textbox(Inches(0.55), Inches(1.05), text_width, Inches(5.6))
    text_frame = body_box.text_frame
    text_frame.word_wrap = True
    text_frame.clear()
    first = True
    for line in [body, *bullets]:
        text = str(line or "").strip()
        if not text:
            continue
        paragraph = text_frame.paragraphs[0] if first else text_frame.add_paragraph()
        paragraph.text = text[:700]
        paragraph.font.size = Pt(17 if first and body else 15)
        if not first or text in bullets:
            paragraph.level = 0
        first = False


def _add_presentation_images(slide, image_paths: list[Path]) -> list[str]:
    from pptx.util import Inches

    embedded: list[str] = []
    if not image_paths:
        return embedded
    max_width = Inches(4.25)
    max_height = Inches(4.85 if len(image_paths) == 1 else 2.25)
    left = Inches(5.25)
    top = Inches(1.18)
    for index, image_path in enumerate(image_paths[:2]):
        image_top = top + Inches(2.45 * index)
        try:
            slide.shapes.add_picture(str(image_path), left, image_top, width=max_width, height=max_height)
            embedded.append(str(image_path))
        except Exception:
            continue
    return embedded


def _build_presentation_create_handler(
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
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "presentation_create requires workspace_root or allowed_roots")
        try:
            from pptx import Presentation
        except ModuleNotFoundError as exc:
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {
                    "reason": "missing_dependency",
                    "dependency_id": "python-pptx",
                    "dependency": "python-pptx",
                    "package": "python-pptx",
                    "requirement": "python-pptx>=1.0,<2",
                    "license": "MIT",
                    "install_command": "python -m pip install 'python-pptx>=1.0,<2'",
                },
                f"presentation_create requires python-pptx: {exc}",
            )

        title = str(invocation.arguments.get("title") or "Nullion presentation").strip() or "Nullion presentation"
        image_paths, image_error = _coerce_string_list(invocation.arguments.get("image_paths"), field="image_paths")
        if image_error is not None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, image_error)
        slides, slide_error = _presentation_slide_specs(invocation.arguments.get("slides"), image_paths, title=title)
        if slide_error is not None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, slide_error)

        output_path = _presentation_output_path(
            invocation,
            raw_path=invocation.arguments.get("output_path"),
            title=title,
            roots=effective_roots,
        )
        if output_path.suffix.lower() != ".pptx":
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {"path": str(output_path)}, "presentation_create output_path must end in .pptx")
        if not _path_within_any_root(output_path, effective_roots) and not _is_approved_filesystem_path(
            output_path,
            invocation.trusted_filesystem_selectors,
        ):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"Path is outside workspace root: {output_path}")

        deck = Presentation()
        blank_layout = deck.slide_layouts[6]
        embedded_images: list[str] = []
        skipped_images: list[str] = []
        for slide_spec in slides:
            slide = deck.slides.add_slide(blank_layout)
            slide_images, slide_skipped, image_error = _resolve_presentation_image_paths(
                list(slide_spec.get("image_paths") or []),
                roots=effective_roots,
                invocation=invocation,
            )
            if image_error is not None:
                return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, image_error)
            skipped_images.extend(slide_skipped)
            _add_presentation_text(
                slide,
                title=str(slide_spec.get("title") or title),
                body=str(slide_spec.get("body") or ""),
                bullets=[str(item) for item in slide_spec.get("bullets") or []],
                has_images=bool(slide_images),
            )
            embedded_images.extend(_add_presentation_images(slide, slide_images))

        output_path.parent.mkdir(parents=True, exist_ok=True)
        deck.save(output_path)
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "path": str(output_path),
                "artifact_path": str(output_path),
                "artifact_paths": [str(output_path)],
                "slide_count": len(slides),
                "embedded_images": embedded_images,
                "skipped_images": skipped_images,
                "bytes_written": output_path.stat().st_size,
            },
            None,
        )

    return handler


_PDF_RENDER_DPI = 300.0
_PDF_JPEG_QUALITY = 95
_PDF_PAGE_SIZES = {
    "letter": (2550, 3300),
    "a4": (2480, 3508),
}


def _pdf_points_to_px(points: float) -> int:
    return max(1, int(round((float(points) * _PDF_RENDER_DPI) / 72.0)))


def _load_pdf_font(*, size_px: int, bold: bool = False):
    from PIL import ImageFont

    candidates = ("DejaVuSans-Bold.ttf", "Arial Bold.ttf", "Arial.ttf") if bold else ("DejaVuSans.ttf", "Arial.ttf")
    for name in candidates:
        try:
            return ImageFont.truetype(name, size=size_px)
        except Exception:
            continue
    return ImageFont.load_default()


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
    from PIL import Image, ImageDraw

    page = Image.new("RGB", page_size, "white")
    draw = ImageDraw.Draw(page)
    body_font = _load_pdf_font(size_px=_pdf_points_to_px(12))
    title_font = _load_pdf_font(size_px=_pdf_points_to_px(18), bold=True)
    margin = _pdf_points_to_px(36)
    max_width = max(120, page_size[0] - margin * 2)
    body_line_height = max(22, int((draw.textbbox((0, 0), "Ag", font=body_font)[3]) * 1.35))
    title_line_height = max(30, int((draw.textbbox((0, 0), "Ag", font=title_font)[3]) * 1.25))

    def wrap_for_width(paragraph: str) -> list[str]:
        words = [word for word in paragraph.split() if word]
        if not words:
            return [""]
        lines: list[str] = []
        current = words[0]
        for word in words[1:]:
            candidate = f"{current} {word}"
            if draw.textlength(candidate, font=body_font) <= max_width:
                current = candidate
            else:
                lines.append(current)
                if draw.textlength(word, font=body_font) <= max_width:
                    current = word
                else:
                    # Emergency fallback for oversized unbroken tokens.
                    hard_lines = textwrap.wrap(word, width=40) or [word]
                    lines.extend(hard_lines[:-1])
                    current = hard_lines[-1]
        lines.append(current)
        return lines

    y = margin
    if title.strip():
        draw.text((margin, y), title.strip()[:140], fill="black", font=title_font)
        y += title_line_height + _pdf_points_to_px(6)
    for paragraph in str(text or "").splitlines() or [""]:
        lines = wrap_for_width(paragraph)
        for line in lines:
            if y > page_size[1] - margin:
                return page
            draw.text((margin, y), line, fill="black", font=body_font)
            y += body_line_height
        y += _pdf_points_to_px(6)
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
        resolution=_PDF_RENDER_DPI,
        quality=_PDF_JPEG_QUALITY,
        subsampling=0,
        optimize=True,
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
        max_entries = _env_int("NULLION_WORKSPACE_SUMMARY_MAX_ENTRIES", 20_000, minimum=1)
        max_sample_files = _env_int("NULLION_WORKSPACE_SUMMARY_SAMPLE_FILES", 40, minimum=0)
        max_extensions = _env_int("NULLION_WORKSPACE_SUMMARY_EXTENSIONS", 40, minimum=1)
        seen_directories: set[Path] = set()
        seen_files: set[Path] = set()
        extensions: Counter[str] = Counter()
        sample_files: list[str] = []
        scanned_entries = 0
        truncated = False

        for root in roots:
            if scanned_entries >= max_entries:
                truncated = True
                break
            for current_dir, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
                if scanned_entries >= max_entries:
                    truncated = True
                    dirnames[:] = []
                    break
                current_path = Path(current_dir)
                scoped_dirnames: list[str] = []
                for dirname in sorted(dirnames):
                    scanned_entries += 1
                    if scanned_entries > max_entries:
                        truncated = True
                        break
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
                    scanned_entries += 1
                    if scanned_entries > max_entries:
                        truncated = True
                        break
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
                    if len(sample_files) < max_sample_files:
                        sample_files.append(display_path)

        extension_rows = [
            {"extension": extension, "count": count}
            for extension, count in sorted(extensions.items(), key=lambda item: (-item[1], item[0]))
        ]

        output = {
            "roots": [str(root) for root in roots],
            "file_count": len(seen_files),
            "directory_count": len(seen_directories),
            "scanned_entries": scanned_entries,
            "truncated": truncated,
            "extensions": extension_rows[:max_extensions],
            "sample_files": sorted(sample_files),
        }
        if len(extension_rows) > max_extensions:
            output["extensions_truncated"] = {"shown": max_extensions, "total": len(extension_rows)}
        if len(seen_files) > max_sample_files:
            output["sample_files_truncated"] = {"shown": max_sample_files, "total": len(seen_files)}

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output=output,
            error=None,
        )

    return handler



def _terminal_deliverable_artifact_paths_since(
    roots: Iterable[Path],
    *,
    since_timestamp: float,
) -> list[str]:
    candidates: list[tuple[float, str]] = []
    seen: set[str] = set()
    scanned_entries = 0
    for raw_root in roots:
        try:
            root = Path(raw_root).expanduser().resolve()
        except (OSError, RuntimeError, ValueError):
            continue
        if not root.is_dir():
            continue
        stack = [root]
        while stack and scanned_entries < _MAX_TERMINAL_ARTIFACT_SCAN_ENTRIES:
            current = stack.pop()
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        scanned_entries += 1
                        if scanned_entries > _MAX_TERMINAL_ARTIFACT_SCAN_ENTRIES:
                            break
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(Path(entry.path))
                                continue
                            if not entry.is_file(follow_symlinks=False):
                                continue
                            path = Path(entry.path).resolve()
                            if path.suffix.lower() not in _TERMINAL_DELIVERABLE_ARTIFACT_EXTENSIONS:
                                continue
                            stat = path.stat()
                        except (OSError, RuntimeError, ValueError):
                            continue
                        if stat.st_size <= 0 or stat.st_mtime < since_timestamp:
                            continue
                        resolved = str(path)
                        if resolved in seen:
                            continue
                        seen.add(resolved)
                        candidates.append((stat.st_mtime, resolved))
            except OSError:
                continue
    candidates.sort(key=lambda item: (item[0], item[1]))
    return promote_supporting_asset_artifact_paths(
        [path for _mtime, path in candidates[-_MAX_TERMINAL_DISCOVERED_ARTIFACTS:]],
        artifact_roots=tuple(Path(root).expanduser().resolve() for root in roots),
    )


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
            principal_id=invocation.principal_id,
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
        shell = _normalize_terminal_shell(invocation.arguments.get("shell"))
        timeout_seconds = _terminal_timeout_seconds(invocation.arguments.get("timeout_seconds"))
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
            started_at = datetime.now(UTC).timestamp()
            try:
                completed = backend.run(
                    raw_command,
                    cwd=str(execution_cwd) if execution_cwd is not None else None,
                    timeout=timeout_seconds,
                    policy=policy,
                    shell=shell,
                )
            except TypeError as exc:
                if "shell" not in str(exc):
                    raise
                completed = backend.run(
                    raw_command,
                    cwd=str(execution_cwd) if execution_cwd is not None else None,
                    timeout=timeout_seconds,
                    policy=policy,
                )
        except subprocess.TimeoutExpired:
            output = {"egress_attempts": egress_attempts} if has_network_egress else {}
            if network_mode is not None:
                output["network_mode"] = network_mode
            output["timeout_seconds"] = timeout_seconds
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=output,
                error=f"Command timed out after {timeout_seconds}s",
            )

        output = {
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "exit_code": completed.exit_code,
            "shell": completed.shell,
            "timeout_seconds": timeout_seconds,
        }
        if network_mode is not None:
            output["network_mode"] = network_mode
        if has_network_egress:
            output["egress_attempts"] = egress_attempts
        if completed.exit_code == 0:
            artifact_paths = _terminal_deliverable_artifact_paths_since(
                resolved_allowed_roots,
                since_timestamp=started_at,
            )
            if artifact_paths:
                output["artifact_paths"] = artifact_paths
        error = None
        if completed.exit_code != 0:
            if completed.exit_code == 127 and completed.stderr.startswith(
                ("Shell startup failed:", "Terminal launcher startup failed:")
            ):
                error = completed.stderr
            else:
                error = f"Command failed with exit code {completed.exit_code}"
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed" if completed.exit_code == 0 else "failed",
            output=output,
            error=error,
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


def _web_fetch_binary_suffix(url: str, content_type: str) -> str:
    parsed_suffix = Path(urlparse(url).path).suffix.lower()
    if parsed_suffix in VALID_ATTACHMENT_EXTENSIONS:
        return parsed_suffix
    guessed = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())
    if guessed:
        return ".jpg" if guessed == ".jpe" else guessed
    return ".bin"


def _web_fetch_is_textual(content_type: str, data: bytes) -> bool:
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type.startswith("text/") or media_type in {
        "application/json",
        "application/javascript",
        "application/xml",
        "application/xhtml+xml",
        "application/rss+xml",
        "application/atom+xml",
        "image/svg+xml",
    }:
        return True
    sample = data[:1024]
    if b"\x00" in sample:
        return False
    try:
        sample.decode("utf-8")
        return media_type in {"", "application/octet-stream"}
    except UnicodeDecodeError:
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
            data = response.read(_WEB_FETCH_MAX_BODY_BYTES + 1)
    truncated = len(data) > _WEB_FETCH_MAX_BODY_BYTES
    if truncated:
        data = data[:_WEB_FETCH_MAX_BODY_BYTES]
    if not _web_fetch_is_textual(content_type, data):
        return {
            "url": url,
            "status_code": status_code,
            "content_type": content_type,
            "content_kind": "binary",
            "body_size": len(data),
            "body_truncated": truncated,
            "suggested_extension": _web_fetch_binary_suffix(url, content_type),
            "_body_bytes": data,
        }
    body = data[:65536].decode("utf-8", "ignore")  # 64 KB of text
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


def register_web_fetch_tool(
    registry: ToolRegistry,
    web_fetcher: Callable[[str, int], dict[str, object]] | None = None,
) -> ToolRegistry:
    try:
        registry.get_spec("web_fetch")
        return registry
    except KeyError:
        pass
    registry.register(
        ToolSpec(
            name="web_fetch",
            description=(
                "Fetch a URL and return its content and response metadata. "
                "Use only when direct HTTP fetching is enabled by the runtime policy; otherwise use browser tools."
            ),
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=20,
        ),
        _build_web_fetch_handler(web_fetcher or _default_web_fetcher),
    )
    return registry


def _json_get(url: str, timeout_seconds: int) -> dict[str, object]:
    request = urllib.request.Request(url, headers={"User-Agent": "Nullion/1.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        payload = response.read(500_000)
    decoded = json.loads(payload.decode("utf-8"))
    return decoded if isinstance(decoded, dict) else {}


def _coerce_float_arg(value: object, *, name: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc


def _resolve_open_meteo_location(
    arguments: dict[str, object],
    *,
    json_get: Callable[[str, int], dict[str, object]],
) -> dict[str, object]:
    latitude = _coerce_float_arg(arguments.get("latitude"), name="latitude")
    longitude = _coerce_float_arg(arguments.get("longitude"), name="longitude")
    location_text = str(arguments.get("location_text") or "").strip()
    if latitude is not None and longitude is not None:
        return {
            "latitude": latitude,
            "longitude": longitude,
            "name": location_text or f"{latitude:.4f},{longitude:.4f}",
            "source_url": None,
        }
    if not location_text:
        raise ValueError("Provide location_text or latitude and longitude.")
    source_url = ""
    results: object = None
    searched_locations: list[str] = []
    candidate_locations = [location_text]
    zip_match = re.search(r"(?<!\d)(\d{5})(?:-\d{4})?(?!\d)", location_text)
    if zip_match:
        zip_code = zip_match.group(1)
        if zip_code != location_text:
            candidate_locations.append(zip_code)
    for candidate_location in dict.fromkeys(candidate_locations):
        searched_locations.append(candidate_location)
        query = urlencode({"name": candidate_location, "count": 1, "language": "en", "format": "json"})
        source_url = f"https://geocoding-api.open-meteo.com/v1/search?{query}"
        payload = json_get(source_url, 10)
        results = payload.get("results")
        if isinstance(results, list) and results:
            break
    if not isinstance(results, list) or not results:
        searched = ", ".join(searched_locations)
        raise ValueError(f"Could not resolve location: {location_text}" + (f" (tried: {searched})" if searched else ""))
    first = results[0]
    if not isinstance(first, dict):
        raise ValueError(f"Could not resolve location: {location_text}")
    resolved_latitude = _coerce_float_arg(first.get("latitude"), name="latitude")
    resolved_longitude = _coerce_float_arg(first.get("longitude"), name="longitude")
    if resolved_latitude is None or resolved_longitude is None:
        raise ValueError(f"Resolved location is missing coordinates: {location_text}")
    label_parts = [
        str(first.get("name") or "").strip(),
        str(first.get("admin1") or "").strip(),
        str(first.get("country") or "").strip(),
    ]
    label = ", ".join(part for part in label_parts if part) or location_text
    return {
        "latitude": resolved_latitude,
        "longitude": resolved_longitude,
        "name": label,
        "country": first.get("country"),
        "timezone": first.get("timezone"),
        "source_url": source_url,
    }


def _weather_code_label(code: object) -> str:
    try:
        value = int(code)
    except (TypeError, ValueError):
        return "unknown"
    labels = {
        0: "clear sky",
        1: "mainly clear",
        2: "partly cloudy",
        3: "overcast",
        45: "fog",
        48: "depositing rime fog",
        51: "light drizzle",
        53: "moderate drizzle",
        55: "dense drizzle",
        56: "light freezing drizzle",
        57: "dense freezing drizzle",
        61: "slight rain",
        63: "moderate rain",
        65: "heavy rain",
        66: "light freezing rain",
        67: "heavy freezing rain",
        71: "slight snow",
        73: "moderate snow",
        75: "heavy snow",
        77: "snow grains",
        80: "slight rain showers",
        81: "moderate rain showers",
        82: "violent rain showers",
        85: "slight snow showers",
        86: "heavy snow showers",
        95: "thunderstorm",
        96: "thunderstorm with slight hail",
        99: "thunderstorm with heavy hail",
    }
    return labels.get(value, f"weather code {value}")


def _daily_open_meteo_forecast(payload: dict[str, object]) -> list[dict[str, object]]:
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        return []
    dates = daily.get("time")
    if not isinstance(dates, list):
        return []
    rows: list[dict[str, object]] = []
    for index, date_value in enumerate(dates):
        row: dict[str, object] = {"date": date_value}
        for source_key, target_key in (
            ("weather_code", "weather_code"),
            ("temperature_2m_max", "temperature_max_f"),
            ("temperature_2m_min", "temperature_min_f"),
            ("precipitation_probability_max", "precipitation_probability_max"),
            ("precipitation_sum", "precipitation_sum_in"),
            ("wind_speed_10m_max", "wind_speed_max_mph"),
            ("sunrise", "sunrise"),
            ("sunset", "sunset"),
        ):
            values = daily.get(source_key)
            if isinstance(values, list) and index < len(values):
                row[target_key] = values[index]
        row["summary"] = _weather_code_label(row.get("weather_code"))
        rows.append(row)
    return rows


def _build_weather_forecast_handler(
    json_get: Callable[[str, int], dict[str, object]],
) -> ToolHandler:
    def _handler(invocation: ToolInvocation) -> ToolResult:
        try:
            location = _resolve_open_meteo_location(invocation.arguments, json_get=json_get)
            forecast_days = invocation.arguments.get("forecast_days", 3)
            try:
                days = min(7, max(1, int(forecast_days)))
            except (TypeError, ValueError):
                days = 3
            timezone = str(invocation.arguments.get("timezone") or location.get("timezone") or "auto").strip() or "auto"
            query = urlencode(
                {
                    "latitude": location["latitude"],
                    "longitude": location["longitude"],
                    "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
                    "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max,precipitation_sum,wind_speed_10m_max,sunrise,sunset",
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                    "precipitation_unit": "inch",
                    "timezone": timezone,
                    "forecast_days": days,
                }
            )
            forecast_url = f"https://api.open-meteo.com/v1/forecast?{query}"
            payload = json_get(forecast_url, 15)
            output = {
                "provider": "open-meteo",
                "source_url": forecast_url,
                "geocoding_source_url": location.get("source_url"),
                "location": {
                    "name": location.get("name"),
                    "country": location.get("country"),
                    "latitude": location.get("latitude"),
                    "longitude": location.get("longitude"),
                    "timezone": payload.get("timezone") or timezone,
                },
                "current": payload.get("current") if isinstance(payload.get("current"), dict) else {},
                "daily": _daily_open_meteo_forecast(payload),
                "units": {
                    "temperature": "fahrenheit",
                    "wind_speed": "mph",
                    "precipitation": "inch",
                },
            }
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", output)
        except Exception as exc:
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {"reason": "weather_forecast_failed"},
                str(exc),
            )

    return _handler


def register_weather_forecast_tool(
    registry: ToolRegistry,
    *,
    json_get: Callable[[str, int], dict[str, object]] | None = None,
) -> ToolRegistry:
    try:
        registry.get_spec("weather_forecast")
        return registry
    except KeyError:
        pass
    registry.register(
        ToolSpec(
            name="weather_forecast",
            description=(
                "Fetch current and multi-day public forecast data from Open-Meteo using structured "
                "coordinates or a resolvable location. Use for read-only weather forecast questions "
                "before browser navigation or general web search when this tool is available."
            ),
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=20,
            capability_tags=("public_web", "weather", "forecast"),
        ),
        _build_weather_forecast_handler(json_get or _json_get),
    )
    return registry



def _build_kernel_tool_registry(
    *,
    plugin_registration_allowed: bool,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
    direct_web_fetch_enabled: bool | None = None,
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
                name="spreadsheet_create",
                description=(
                    "Create a real .xlsx spreadsheet artifact from structured rows, links, and existing image files. "
                    "Use this for spreadsheet delivery instead of terminal_exec. "
                    "If the tool reports missing_dependency, ask the user to approve installing the listed open-source package."
                ),
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=30,
            ),
            _build_spreadsheet_create_handler(
                None
                if workspace_root is None
                else Path(workspace_root),
                allowed_roots=[Path(root) for root in allowed_roots] if allowed_roots is not None else None,
            ),
        )
        registry.register(
            ToolSpec(
                name="presentation_create",
                description=(
                    "Create a real .pptx slide deck artifact from structured slides and existing image files. "
                    "Use this for PowerPoint or presentation delivery instead of terminal_exec. "
                    "If the tool reports missing_dependency, ask the user to approve installing the listed open-source package."
                ),
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=30,
            ),
            _build_presentation_create_handler(
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
    if _env_flag("NULLION_WEB_ACCESS_ENABLED") and (
        direct_web_fetch_enabled
        if direct_web_fetch_enabled is not None
        else _env_flag("NULLION_DIRECT_WEB_FETCH_ENABLED", default=False)
    ):
        register_web_fetch_tool(registry)
    if _env_flag("NULLION_WEB_ACCESS_ENABLED"):
        register_weather_forecast_tool(registry)
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
                capability_tags=("skill_pack",),
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
    direct_web_fetch_enabled: bool | None = None,
) -> ToolRegistry:
    return _build_kernel_tool_registry(
        plugin_registration_allowed=False,
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
        terminal_executor_backend=terminal_executor_backend,
        terminal_attestation_verifier=terminal_attestation_verifier,
        direct_web_fetch_enabled=direct_web_fetch_enabled,
    )



def create_plugin_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
    direct_web_fetch_enabled: bool | None = None,
) -> ToolRegistry:
    return _build_kernel_tool_registry(
        plugin_registration_allowed=True,
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
        terminal_executor_backend=terminal_executor_backend,
        terminal_attestation_verifier=terminal_attestation_verifier,
        direct_web_fetch_enabled=direct_web_fetch_enabled,
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

        body_bytes = response.pop("_body_bytes", None)
        if isinstance(body_bytes, bytes):
            try:
                from nullion.artifacts import artifact_path_for_generated_workspace_file

                suffix = str(response.get("suggested_extension") or ".bin")
                path = artifact_path_for_generated_workspace_file(
                    principal_id=invocation.principal_id,
                    suffix=suffix,
                    stem="web-fetch",
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body_bytes)
                response["artifact_path"] = str(path)
                response["artifact_paths"] = [str(path)]
            except Exception as exc:
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output=response,
                    error=f"Could not materialize binary web_fetch response: {exc}",
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
    except KeyError:
        registry.mark_plugin_installed("connector_plugin")
        registry.register(
            ToolSpec(
                name="connector_request",
                description=(
                    "Make an HTTP request to an installed connector/API gateway using the current workspace's "
                    "configured connection credential. GET/HEAD are always read-only; POST, PUT, PATCH, and "
                    "DELETE require that connection's permission mode to be read_write. Use enabled connector "
                    "skill instructions, keep requests under that provider's configured base URL, and never "
                    "reveal the credential value. Use public web/browser tools for generic public URLs instead."
                ),
                risk_level=ToolRiskLevel.HIGH,
                side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
                requires_approval=False,
                timeout_seconds=20,
                capability_tags=("connector",),
            ),
            _build_connector_request_handler(),
        )
    try:
        registry.get_spec("email_search")
    except KeyError:
        registry.mark_plugin_installed("connector_plugin")
        registry.register(
            ToolSpec(
                name="email_search",
                description=(
                    "Search messages through an active Google Mail connector. Use this for inbox checks, "
                    "triage, and finding messages before reading them."
                ),
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
                capability_tags=("email", "connector", "account_read"),
            ),
            _build_connector_email_search_handler(),
        )
    try:
        registry.get_spec("email_read")
    except KeyError:
        registry.mark_plugin_installed("connector_plugin")
        registry.register(
            ToolSpec(
                name="email_read",
                description="Read one message through an active Google Mail connector using an id from email_search.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
                capability_tags=("email", "connector", "account_read"),
            ),
            _build_connector_email_read_handler(),
        )
    try:
        registry.get_spec("calendar_list")
    except KeyError:
        registry.mark_plugin_installed("connector_plugin")
        registry.register(
            ToolSpec(
                name="calendar_list",
                description=(
                    "List calendar events through an active Google Calendar connector for a specific time window. "
                    "Use this for checking the user's calendar, agenda, schedule, or availability."
                ),
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
                capability_tags=("calendar", "connector", "account_read"),
            ),
            _build_connector_calendar_list_handler(),
        )
    try:
        registry.get_spec("email_send")
        return
    except KeyError:
        pass
    registry.mark_plugin_installed("connector_plugin")
    registry.register(
        ToolSpec(
            name="email_send",
            description=(
                "Send a plain-text email, optionally with local artifact/media attachments, through an active "
                "write-capable Google Mail connector. Use this for actual email delivery; use connector_request "
                "only for lower-level connector APIs."
            ),
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=20,
            capability_tags=("email", "connector"),
        ),
        _build_connector_email_send_handler(),
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


def _string_list_argument(raw_value: object) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, str):
        value = raw_value.strip()
        return [value] if value else []
    if isinstance(raw_value, (list, tuple)):
        values: list[str] = []
        for item in raw_value:
            value = str(item or "").strip()
            if value:
                values.append(value)
        return values
    value = str(raw_value or "").strip()
    return [value] if value else []


def _default_email_connector_provider_id(principal_id: str | None) -> str:
    try:
        from nullion.connections import default_email_connector_provider_id

        provider_id = default_email_connector_provider_id(principal_id)
        if provider_id:
            return provider_id
    except Exception:
        pass
    raise RuntimeError("No active Google Mail connector is available for this workspace/principal.")


def _default_calendar_connector_provider_id(principal_id: str | None) -> str:
    try:
        from nullion.connections import connection_for_principal, load_connection_registry
    except Exception:
        raise RuntimeError("No active calendar connector is available for this workspace/principal.")
    fallback_provider_id = ""
    try:
        connections = load_connection_registry().connections
    except Exception:
        connections = ()
    for connection in connections:
        provider_id = str(getattr(connection, "provider_id", "") or "").strip()
        if not provider_id or not getattr(connection, "active", True):
            continue
        lowered_provider = provider_id.lower()
        if not (lowered_provider.startswith("skill_pack_connector_") or lowered_provider.endswith("_connector_provider")):
            continue
        scoped_connection = connection_for_principal(principal_id, provider_id)
        if scoped_connection is None:
            continue
        display_name = str(getattr(scoped_connection, "display_name", "") or "").lower()
        if "calendar" in lowered_provider or "calendar" in display_name:
            return provider_id
        if not fallback_provider_id:
            fallback_provider_id = provider_id
    if fallback_provider_id:
        return fallback_provider_id
    raise RuntimeError("No active calendar connector is available for this workspace/principal.")


def _email_send_endpoint_for_provider(connection: object | None, provider_id: str) -> str:
    base_urls = _connector_allowed_base_urls(connection, provider_id)
    for base_url in base_urls:
        parsed = urlparse(base_url)
        if parsed.scheme and parsed.netloc and "maton.ai" in parsed.netloc.lower():
            return f"{parsed.scheme}://{parsed.netloc}/google-mail/gmail/v1/users/me/messages/send"
    return "https://api.maton.ai/google-mail/gmail/v1/users/me/messages/send"


def _email_messages_endpoint_for_provider(connection: object | None, provider_id: str) -> str:
    base_urls = _connector_allowed_base_urls(connection, provider_id)
    for base_url in base_urls:
        parsed = urlparse(base_url)
        if parsed.scheme and parsed.netloc and "maton.ai" in parsed.netloc.lower():
            return f"{parsed.scheme}://{parsed.netloc}/google-mail/gmail/v1/users/me/messages"
    return "https://api.maton.ai/google-mail/gmail/v1/users/me/messages"


def _calendar_events_endpoint_for_provider(connection: object | None, provider_id: str) -> str:
    base_urls = _connector_allowed_base_urls(connection, provider_id)
    for base_url in base_urls:
        parsed = urlparse(base_url)
        if parsed.scheme and parsed.netloc and "maton.ai" in parsed.netloc.lower():
            return f"{parsed.scheme}://{parsed.netloc}/google-calendar/calendar/v3/calendars/primary/events"
    return "https://api.maton.ai/google-calendar/calendar/v3/calendars/primary/events"


def _connector_json_request(
    invocation: ToolInvocation,
    *,
    provider_id: str,
    url: str,
    params: dict[str, object] | None = None,
    method: str = "GET",
    json_payload: object | None = None,
) -> dict[str, object]:
    connection = _connector_connection_for_invocation(invocation, provider_id)
    normalized_method = _connector_request_method(method)
    if normalized_method not in _CONNECTOR_READ_METHODS and not _connector_connection_allows_write(connection):
        raise RuntimeError(f"{provider_id} is configured as read-only for connector_request.")
    request_url = _connector_request_url(url, params or {}, connection, provider_id)
    resolution = _resolve_web_fetch_resolution(request_url)
    headers = _connector_request_headers(connection, provider_id)
    data = None
    if json_payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(json_payload).encode("utf-8")
    request = urllib.request.Request(request_url, data=data, headers=headers, method=normalized_method)
    opener = _build_web_fetch_opener()
    with _pinned_web_fetch_resolution(resolution):
        with opener.open(request, timeout=_connector_request_timeout_seconds()) as response:
            body = response.read(1_000_000).decode("utf-8", "ignore")
    payload = json.loads(body or "{}")
    if not isinstance(payload, dict):
        raise RuntimeError("connector returned non-object JSON")
    return payload


def _connector_request_timeout_seconds() -> int:
    try:
        raw = int(os.environ.get("NULLION_CONNECTOR_REQUEST_TIMEOUT_SECONDS", "12"))
    except ValueError:
        raw = 12
    return max(3, min(raw, 30))


def _gmail_header_map(message: dict[str, object]) -> dict[str, str]:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return {}
    headers = payload.get("headers")
    if not isinstance(headers, list):
        return {}
    wanted = {"from", "to", "cc", "subject", "date"}
    mapped: dict[str, str] = {}
    for header in headers:
        if not isinstance(header, dict):
            continue
        name = str(header.get("name") or "").strip().lower()
        value = str(header.get("value") or "").strip()
        if name in wanted and value:
            mapped[name] = value
    return mapped


def _gmail_decode_body_data(data: object) -> str:
    if not isinstance(data, str) or not data.strip():
        return ""
    padded = data + ("=" * ((4 - len(data) % 4) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8", "replace")
    except Exception:
        return ""


def _gmail_message_body_text(message: dict[str, object], *, limit: int = 6000) -> str:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return ""
    candidates: list[tuple[str, str]] = []

    def visit(part: object) -> None:
        if not isinstance(part, dict):
            return
        mime_type = str(part.get("mimeType") or "").strip().lower()
        body = part.get("body")
        if isinstance(body, dict):
            text = _gmail_decode_body_data(body.get("data"))
            if text.strip():
                candidates.append((mime_type, text))
        for child in part.get("parts") if isinstance(part.get("parts"), list) else []:
            visit(child)

    visit(payload)
    text = next((value for mime, value in candidates if mime == "text/plain"), "")
    if not text:
        html = next((value for mime, value in candidates if mime == "text/html"), "")
        text = re.sub(r"<[^>]+>", " ", html)
        text = unescape(text)
    if not text and candidates:
        text = candidates[0][1]
    return " ".join(text.split())[:limit]


def _compact_gmail_message(message: dict[str, object], *, include_body: bool = False) -> dict[str, object]:
    compact: dict[str, object] = {
        "id": message.get("id"),
        "threadId": message.get("threadId"),
    }
    headers = _gmail_header_map(message)
    if headers:
        compact["headers"] = headers
        for key in ("from", "subject", "date"):
            if key in headers:
                compact[key] = headers[key]
    snippet = message.get("snippet")
    if isinstance(snippet, str) and snippet.strip():
        compact["snippet"] = snippet.strip()
    label_ids = message.get("labelIds")
    if isinstance(label_ids, list):
        compact["labelIds"] = [str(item) for item in label_ids[:12]]
    if include_body:
        body = _gmail_message_body_text(message)
        if body:
            compact["body"] = body
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _compact_google_calendar_event(event: dict[str, object]) -> dict[str, object]:
    compact: dict[str, object] = {
        "id": event.get("id"),
        "summary": event.get("summary"),
        "status": event.get("status"),
        "start": event.get("start"),
        "end": event.get("end"),
        "location": event.get("location"),
        "description": event.get("description"),
        "htmlLink": event.get("htmlLink"),
    }
    attendees = event.get("attendees")
    if isinstance(attendees, list):
        compact["attendees"] = [
            {
                key: attendee.get(key)
                for key in ("email", "displayName", "responseStatus")
                if isinstance(attendee, dict) and attendee.get(key) not in (None, "", [], {})
            }
            for attendee in attendees[:20]
            if isinstance(attendee, dict)
        ]
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _email_message_for_invocation(invocation: ToolInvocation) -> tuple[EmailMessage, list[str]]:
    recipients = _string_list_argument(invocation.arguments.get("to"))
    if not recipients:
        raise ValueError("Missing required argument: to")
    subject = str(invocation.arguments.get("subject") or "").strip()
    body = str(invocation.arguments.get("body") or "")
    msg = EmailMessage()
    msg["To"] = ", ".join(recipients)
    cc = _string_list_argument(invocation.arguments.get("cc"))
    bcc = _string_list_argument(invocation.arguments.get("bcc"))
    if cc:
        msg["Cc"] = ", ".join(cc)
    if bcc:
        msg["Bcc"] = ", ".join(bcc)
    msg["Subject"] = subject
    msg.set_content(body)

    attached_paths: list[str] = []
    for raw_path in _string_list_argument(invocation.arguments.get("attachment_paths")):
        resolved = Path(_resolve_virtual_workspace_path(raw_path, principal_id=invocation.principal_id)).expanduser()
        if not resolved.exists() or not resolved.is_file():
            raise FileNotFoundError(f"Attachment path does not exist or is not a file: {raw_path}")
        content_type, _encoding = mimetypes.guess_type(str(resolved))
        maintype, subtype = (content_type or "application/octet-stream").split("/", 1)
        msg.add_attachment(
            resolved.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            filename=resolved.name,
        )
        attached_paths.append(str(resolved))
    return msg, attached_paths


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


def _build_connector_email_search_handler() -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_query = invocation.arguments.get("query")
        raw_limit = invocation.arguments.get("limit", 10)
        if not isinstance(raw_query, str) or not raw_query.strip():
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
        limit = min(raw_limit, 10)
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        try:
            if not provider_id:
                provider_id = _default_email_connector_provider_id(invocation.principal_id)
            connection = _connector_connection_for_invocation(invocation, provider_id)
            endpoint = _email_messages_endpoint_for_provider(connection, provider_id)
            listing = _connector_json_request(
                invocation,
                provider_id=provider_id,
                url=endpoint,
                params={"q": raw_query.strip(), "maxResults": limit},
            )
            raw_messages = listing.get("messages")
            messages = raw_messages if isinstance(raw_messages, list) else []
            results: list[dict[str, object]] = []
            for item in messages[:limit]:
                if not isinstance(item, dict):
                    continue
                message_id = str(item.get("id") or "").strip()
                if not message_id:
                    continue
                detail = _connector_json_request(
                    invocation,
                    provider_id=provider_id,
                    url=f"{endpoint}/{quote(message_id, safe='')}",
                    params={"format": "full"},
                )
                results.append(_compact_gmail_message(detail, include_body=False))
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={
                    "query": raw_query.strip(),
                    "provider_id": provider_id,
                    "resultSizeEstimate": listing.get("resultSizeEstimate"),
                    "results": results,
                },
                error=None,
            )
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=_account_tool_failure_output(invocation.principal_id, query=raw_query),
                error=str(exc),
            )

    return handler


def _build_connector_email_read_handler() -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_id = invocation.arguments.get("id")
        if not isinstance(raw_id, str) or not raw_id.strip():
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: id",
            )
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        try:
            if not provider_id:
                provider_id = _default_email_connector_provider_id(invocation.principal_id)
            connection = _connector_connection_for_invocation(invocation, provider_id)
            endpoint = _email_messages_endpoint_for_provider(connection, provider_id)
            message = _connector_json_request(
                invocation,
                provider_id=provider_id,
                url=f"{endpoint}/{quote(raw_id.strip(), safe='')}",
                params={"format": "full"},
            )
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={
                    "id": raw_id.strip(),
                    "provider_id": provider_id,
                    "message": _compact_gmail_message(message, include_body=True),
                },
                error=None,
            )
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=_account_tool_failure_output(invocation.principal_id, message_id=raw_id),
                error=str(exc),
            )

    return handler


def _build_connector_calendar_list_handler() -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_start = invocation.arguments.get("start")
        raw_end = invocation.arguments.get("end")
        raw_max = invocation.arguments.get("max", 10)
        if not isinstance(raw_start, str) or not raw_start.strip():
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: start",
            )
        if not isinstance(raw_end, str) or not raw_end.strip():
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
        limit = min(raw_max, 50)
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        try:
            if not provider_id:
                provider_id = _default_calendar_connector_provider_id(invocation.principal_id)
            connection = _connector_connection_for_invocation(invocation, provider_id)
            endpoint = _calendar_events_endpoint_for_provider(connection, provider_id)
            listing = _connector_json_request(
                invocation,
                provider_id=provider_id,
                url=endpoint,
                params={
                    "timeMin": raw_start.strip(),
                    "timeMax": raw_end.strip(),
                    "maxResults": limit,
                    "singleEvents": "true",
                    "orderBy": "startTime",
                },
            )
            raw_items = listing.get("items")
            items = raw_items if isinstance(raw_items, list) else []
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={
                    "provider_id": provider_id,
                    "start": raw_start.strip(),
                    "end": raw_end.strip(),
                    "max": limit,
                    "result_count": len(items[:limit]),
                    "results": [
                        _compact_google_calendar_event(item)
                        for item in items[:limit]
                        if isinstance(item, dict)
                    ],
                },
                error=None,
            )
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=_account_tool_failure_output(invocation.principal_id),
                error=str(exc),
            )

    return handler


def _build_connector_email_send_handler() -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        try:
            if not provider_id:
                provider_id = _default_email_connector_provider_id(invocation.principal_id)
            connection = _connector_connection_for_invocation(invocation, provider_id)
            if not _connector_connection_allows_write(connection):
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output={"provider_id": provider_id, "permission_mode": "read"},
                    error=(
                        f"{provider_id} is configured as read-only for email_send. "
                        "Change this connection's permission mode to read_write in Settings > Users > Connections "
                        "before sending email."
                    ),
                )
            message, attached_paths = _email_message_for_invocation(invocation)
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii").rstrip("=")
            endpoint = _email_send_endpoint_for_provider(connection, provider_id)
            url = _connector_request_url(endpoint, None, connection, provider_id)
            resolution = _resolve_web_fetch_resolution(url)
            payload = json.dumps({"raw": raw_message}).encode("utf-8")
            headers = _connector_request_headers(connection, provider_id)
            headers["Content-Type"] = "application/json"
            request = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            opener = _build_web_fetch_opener()
            with _pinned_web_fetch_resolution(resolution):
                with opener.open(request, timeout=20) as response:
                    status_code = getattr(response, "status", 200)
                    content_type = response.headers.get_content_type()
                    body = response.read(1_000_000).decode("utf-8", "ignore")
            output: dict[str, object] = {
                "url": url,
                "provider_id": provider_id,
                "method": "POST",
                "status_code": status_code,
                "content_type": content_type,
                "to": _string_list_argument(invocation.arguments.get("to")),
                "subject": str(invocation.arguments.get("subject") or "").strip(),
                "attachment_count": len(attached_paths),
                "attachment_paths": attached_paths,
            }
            try:
                parsed_json = json.loads(body)
                if isinstance(parsed_json, dict):
                    output["json"] = parsed_json
                else:
                    output["json"] = parsed_json
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
                output={"query": raw_query, "reason": "web_search_failed"},
                error=str(exc),
            )

        usable_results = [
            result
            for result in results
            if _web_search_result_has_usable_evidence(result)
        ]
        if not usable_results:
            candidates = [
                _web_search_candidate_for_fallback(result)
                for result in results[:limit]
            ]
            candidates = [candidate for candidate in candidates if candidate is not None]
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={
                    "query": raw_query,
                    "reason": "no_usable_search_results",
                    "result_count": len(results),
                    "candidates": candidates,
                },
                error="web_search returned result rows without usable source evidence",
            )

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"query": raw_query, "results": usable_results},
            error=None,
        )

    return handler


def _web_search_result_has_usable_evidence(result: dict[str, object]) -> bool:
    url = result.get("url")
    if not isinstance(url, str) or not url.strip():
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    snippet = str(
        result.get("snippet")
        or result.get("summary")
        or result.get("description")
        or ""
    ).strip()
    return bool(snippet)


def _web_search_candidate_for_fallback(result: dict[str, object]) -> dict[str, str] | None:
    url = result.get("url")
    if not isinstance(url, str) or not url.strip():
        return None
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    candidate: dict[str, str] = {"url": url}
    title = result.get("title")
    if isinstance(title, str) and title.strip():
        candidate["title"] = title.strip()
    return candidate



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
        seen_matches: set[str] = set()
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
                    resolved_text = str(resolved_path)
                    if resolved_text in seen_matches:
                        continue
                    seen_matches.add(resolved_text)
                    matches.append(resolved_text)
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
                        "Search the web and return ranked source links with evidence snippets. "
                        "Use this when no specific URL is already available. If the result has "
                        "no usable evidence snippets, treat it as insufficient for a final answer "
                        "and continue from structured candidate URLs or another structured source."
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
                capability_tags=("email", "connector", "account_read"),
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
                capability_tags=("email", "connector", "account_read"),
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
            capability_tags=("calendar", "connector", "account_read"),
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
            "setup": format_setup_tip(MEDIA_PROVIDER_SETUP_TIP),
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
        if image_generator is not None:
            registry.register(
                ToolSpec(
                    name="image_generate",
                    description="Generate an image file with the configured local image generation provider.",
                    risk_level=ToolRiskLevel.MEDIUM,
                    side_effect_class=ToolSideEffectClass.WRITE,
                    requires_approval=False,
                    timeout_seconds=120,
                    capability_tags=("media", "image_generation"),
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
                    "and return page metadata. Use this only with a URL from explicit "
                    "runtime evidence, structured tool output, or a model-produced "
                    "structured recovery plan."
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
        from nullion.reminders import reminder_due_at_output

        try:
            all_reminders = runtime.store.list_reminders()
            pending = [
                {
                    "task_id": r.task_id,
                    "text": r.text,
                    "due_at": r.due_at.isoformat(),
                    **reminder_due_at_output(r.due_at),
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
            capability_tags=("scheduler", "reminder"),
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
            description=(
                "List all pending one-off reminders. This is not for scheduled cron jobs; "
                "use list_crons/run_cron for cron jobs."
            ),
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=10,
            capability_tags=("scheduler", "reminder"),
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        ),
        _build_list_reminders_handler(runtime),
    )


# ── Cron tools ────────────────────────────────────────────────────────────────

def _build_create_cron_handler(*, default_delivery_channel: str = "", default_delivery_target: str = ""):
    def _current_delivery_context_defaults() -> tuple[str, str]:
        try:
            from nullion.cron_delivery import normalize_cron_delivery_channel
            from nullion.reminders import current_reminder_chat_id

            chat_id = str(current_reminder_chat_id() or "").strip()
            channel, separator, target = chat_id.partition(":")
            normalized_channel = normalize_cron_delivery_channel(channel)
            if separator and normalized_channel and target.strip():
                return normalized_channel, target.strip()
        except Exception:
            pass
        return "", ""

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
        from nullion.crons import add_cron, cron_display_fields
        args = invocation.arguments or {}
        name     = str(args.get("name", "")).strip()
        schedule = str(args.get("schedule", "")).strip()
        task     = str(args.get("task", "")).strip()
        enabled  = bool(args.get("enabled", True))
        workspace_id = _workspace_id_from_invocation(invocation, args)
        context_channel, context_target = _current_delivery_context_defaults()
        configured_default_channel = normalize_cron_delivery_channel(default_delivery_channel)
        default_channel = context_channel or configured_default_channel
        default_target = context_target if context_channel else str(default_delivery_target or "").strip()
        delivery_channel = normalize_cron_delivery_channel(args.get("delivery_channel")) or default_channel
        explicit_target = str(args.get("delivery_target") or "").strip()
        delivery_target = explicit_target
        if not delivery_target and (not delivery_channel or delivery_channel == default_channel):
            delivery_target = default_target
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
        display = cron_display_fields(job)
        schedule_description = display["schedule_description"]
        next_description = display["next_run_description"]
        next_info = f" Next run: {next_description}." if next_description else ""
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
                "schedule_description": schedule_description,
                "next_run_description": next_description,
                "message": (
                    f"Cron created: '{job.name}' in workspace {job.workspace_id}. "
                    f"Schedule: {schedule_description}.{next_info}"
                ),
            },
            error=None,
        )
    return handle


def _build_list_crons_handler():
    def _cron_display_line(index: int, job: object, display: dict[str, str]) -> str:
        name = str(getattr(job, "name", "") or "Untitled scheduled task").strip()
        schedule = str(getattr(job, "schedule", "") or "").strip()
        workspace_id = str(getattr(job, "workspace_id", "") or "").strip()
        enabled = bool(getattr(job, "enabled", False))
        next_run = str(getattr(job, "next_run", "") or "").strip()
        status = "enabled" if enabled else "disabled"
        parts = [f"{index}. {name}", f"Status: {status}"]
        if schedule:
            parts.append(f"Schedule: {display['schedule_description']}")
        if next_run:
            next_description = display["next_run_description"] or next_run
            parts.append(f"Next run: {next_description}")
        if workspace_id:
            parts.append(f"Workspace: {workspace_id}")
        parts.append(f'Run by name: run_cron name="{name}"')
        return " · ".join(parts)

    def handle(invocation: ToolInvocation) -> ToolResult:
        from nullion.connections import workspace_id_for_principal
        from nullion.crons import cron_display_fields, cron_display_timezone, list_crons

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
        cron_tz = cron_display_timezone()
        for index, j in enumerate(jobs, start=1):
            display = cron_display_fields(j, tz=cron_tz)
            lines.append(_cron_display_line(index, j, display))
            run_by_name = f'run_cron name="{j.name}"'
            crons.append(
                {
                    "selection_index": index,
                    "id": j.id,
                    "name": j.name,
                    "display_name": j.name,
                    "workspace_id": j.workspace_id,
                    "delivery_channel": j.delivery_channel,
                    "delivery_target": j.delivery_target,
                    "enabled": j.enabled,
                    "schedule_description": display["schedule_description"],
                    "next_run_description": display["next_run_description"],
                    "last_run": j.last_run,
                    "run_by_name": run_by_name,
                    "presentation_hint": (
                        "Show schedule_description and next_run_description for timing. "
                        "Do not show cron expressions, raw ids, ISO timestamps, or UTC conversions unless the user asks for technical details. "
                        "When asking the user to choose, show numbered options and accept the number."
                    ),
                    "task": j.task,
                    "has_task": bool(str(j.task or "").strip()),
                    "has_last_result": bool(str(j.last_result or "").strip()),
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


def _cron_admin_workspace_allowed(workspace_id: str) -> bool:
    return str(workspace_id or "").strip() == "workspace_admin"


def _cron_workspace_denial(
    *,
    invocation: ToolInvocation,
    cron_id: str,
    owner_workspace_id: str,
) -> ToolResult:
    return ToolResult(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        status="failed",
        output={"id": cron_id, "workspace_id": owner_workspace_id},
        error=f"Cron {cron_id!r} belongs to workspace {owner_workspace_id}.",
    )


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
        admin_cross_workspace = job is not None and job.workspace_id != workspace_id and _cron_admin_workspace_allowed(workspace_id)
        if job is not None and job.workspace_id != workspace_id and not admin_cross_workspace:
            return _cron_workspace_denial(invocation=invocation, cron_id=cron_id, owner_workspace_id=job.workspace_id)
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
            output={
                "id": cron_id,
                "admin_cross_workspace": admin_cross_workspace,
                "message": f"Cron {cron_id} deleted.",
            },
            error=None,
        )
    return handle


def _build_update_cron_handler():
    def handle(invocation: ToolInvocation) -> ToolResult:
        from nullion.connections import workspace_id_for_principal
        from nullion.crons import get_cron, update_cron

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
        existing = get_cron(cron_id)
        workspace_id = workspace_id_for_principal(invocation.principal_id)
        admin_cross_workspace = (
            existing is not None
            and existing.workspace_id != workspace_id
            and _cron_admin_workspace_allowed(workspace_id)
        )
        if existing is not None and existing.workspace_id != workspace_id and not admin_cross_workspace:
            return _cron_workspace_denial(invocation=invocation, cron_id=cron_id, owner_workspace_id=existing.workspace_id)
        mutable_fields = ("name", "schedule", "task", "enabled", "delivery_channel", "delivery_target", "workspace_id")
        updates: dict[str, object] = {}
        for field in mutable_fields:
            if field not in args:
                continue
            value = args[field]
            if field in {"name", "schedule", "task", "delivery_channel", "delivery_target", "workspace_id"}:
                value = str(value or "").strip()
            updates[field] = value
        if not updates:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": cron_id},
                error="at least one cron field is required",
            )
        try:
            job = update_cron(cron_id, **updates)
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": cron_id},
                error=f"Failed to update cron: {exc}",
            )
        if job is None:
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
            output={
                "id": job.id,
                "name": job.name,
                "schedule": job.schedule,
                "workspace_id": job.workspace_id,
                "delivery_channel": job.delivery_channel,
                "delivery_target": job.delivery_target,
                "enabled": job.enabled,
                "admin_cross_workspace": admin_cross_workspace,
                "next_run": job.next_run,
                "has_task": bool(str(job.task or "").strip()),
                "has_last_result": bool(str(job.last_result or "").strip()),
                "message": f"Cron updated: '{job.name}' (id={job.id}).",
            },
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
        admin_cross_workspace = (
            existing is not None
            and existing.workspace_id != workspace_id
            and _cron_admin_workspace_allowed(workspace_id)
        )
        if existing is not None and existing.workspace_id != workspace_id and not admin_cross_workspace:
            return _cron_workspace_denial(invocation=invocation, cron_id=cron_id, owner_workspace_id=existing.workspace_id)
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
                "admin_cross_workspace": admin_cross_workspace,
                "message": f"Cron '{job.name}' ({cron_id}) is now {state}.",
            },
            error=None,
        )
    return handle


def _build_run_cron_handler(cron_runner: Callable[..., str | dict[str, object] | None] | None):
    def _foreground_cron_no_output_text() -> str:
        from nullion.cron_delivery import DEFAULT_CRON_NO_OUTPUT_MESSAGE

        return DEFAULT_CRON_NO_OUTPUT_MESSAGE

    def _cron_lookup_parts(value: object) -> tuple[str, tuple[str, ...]]:
        from nullion.text_match import ascii_match_text

        text = ascii_match_text(value).casefold()
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
        expanded_tokens: list[str] = []
        for token in tokens:
            expanded_tokens.append(token)
            for suffix in ("ing", "ers", "er", "ed", "s"):
                if token.endswith(suffix) and len(token) - len(suffix) >= 4:
                    expanded_tokens.append(token[: -len(suffix)])
        return "".join(tokens), tuple(dict.fromkeys(expanded_tokens))

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

    def _cron_reference_match_rank(query: str, candidate_text: str) -> int | None:
        rank = _cron_name_match_rank(query, candidate_text)
        if rank is not None:
            return rank
        _query_compact, query_tokens = _cron_lookup_parts(query)
        _candidate_compact, candidate_tokens = _cron_lookup_parts(candidate_text)
        overlap = set(query_tokens).intersection(candidate_tokens)
        if len(overlap) >= 2:
            return 5
        fuzzy_overlap = _fuzzy_cron_token_overlap(query_tokens, candidate_tokens)
        if len(fuzzy_overlap) >= 1 and len(query_tokens) <= 4:
            return 6
        return None

    def _fuzzy_cron_token_overlap(query_tokens: tuple[str, ...], candidate_tokens: tuple[str, ...]) -> set[str]:
        from difflib import SequenceMatcher

        matches: set[str] = set()
        candidate_set = {token for token in candidate_tokens if len(token) >= 6}
        for query_token in query_tokens:
            if len(query_token) < 6:
                continue
            for candidate_token in candidate_set:
                length_delta = abs(len(query_token) - len(candidate_token))
                if length_delta > max(2, len(candidate_token) // 4):
                    continue
                if SequenceMatcher(None, query_token, candidate_token).ratio() >= 0.86:
                    matches.add(query_token)
                    break
        return matches

    def _significant_cron_tokens(value: object) -> set[str]:
        _compact, tokens = _cron_lookup_parts(value)
        return {token for token in tokens if len(token) >= 5}

    def _cron_search_text(candidate: object) -> str:
        return " ".join(
            (
                str(getattr(candidate, "name", "") or ""),
                str(getattr(candidate, "task", "") or ""),
            )
        )

    def _descriptive_cron_matches(query: str, jobs: list[object]) -> list[object]:
        query_tokens = _significant_cron_tokens(query)
        if not query_tokens:
            return []
        scored: list[tuple[int, object]] = []
        for candidate in jobs:
            if not bool(getattr(candidate, "enabled", True)):
                continue
            candidate_tokens = _significant_cron_tokens(_cron_search_text(candidate))
            overlap = query_tokens.intersection(candidate_tokens)
            fuzzy_overlap = _fuzzy_cron_token_overlap(tuple(query_tokens), tuple(candidate_tokens))
            score = len(overlap) * 2 + len(fuzzy_overlap)
            if score:
                scored.append((score, candidate))
        if not scored:
            return []
        best_score = max(score for score, _candidate in scored)
        return [candidate for score, candidate in scored if score == best_score]

    def _unique_cron_name_match(query: str, jobs: list[object]) -> tuple[object | None, list[object]]:
        ranked: list[tuple[int, object]] = []
        for candidate in jobs:
            rank = _cron_name_match_rank(query, str(getattr(candidate, "name", "") or ""))
            if rank is None and bool(getattr(candidate, "enabled", True)):
                search_rank = _cron_reference_match_rank(query, _cron_search_text(candidate))
                if search_rank is not None:
                    rank = search_rank + 2
            if rank is not None:
                ranked.append((rank, candidate))
        if not ranked:
            descriptive_matches = _descriptive_cron_matches(query, jobs)
            if len(descriptive_matches) == 1:
                return descriptive_matches[0], descriptive_matches
            if descriptive_matches:
                return None, descriptive_matches
            return None, []
        best_rank = min(rank for rank, _candidate in ranked)
        matches = [candidate for rank, candidate in ranked if rank == best_rank]
        if len(matches) == 1:
            return matches[0], matches
        descriptive_matches = _descriptive_cron_matches(query, matches)
        if len(descriptive_matches) == 1:
            return descriptive_matches[0], descriptive_matches
        if descriptive_matches:
            return None, descriptive_matches
        return None, matches

    def _numbered_cron_matches(matches: list[object]) -> list[dict[str, object]]:
        return [
            {
                "selection_index": index,
                "id": item.id,
                "name": item.name,
                "schedule": item.schedule,
                "workspace_id": item.workspace_id,
                "enabled": item.enabled,
                "next_run": item.next_run,
                "reply_with": str(index),
            }
            for index, item in enumerate(matches, start=1)
        ]

    def _foreground_cron_result_view(text: str) -> tuple[str, int]:
        from nullion.artifacts import parse_media_directive_line

        removed_media_count = 0
        kept_lines: list[str] = []
        for raw_line in str(text or "").splitlines():
            directive = parse_media_directive_line(raw_line)
            if directive is not None:
                removed_media_count += 1
                continue
            kept_lines.append(raw_line)
        if removed_media_count:
            return "", removed_media_count
        return "\n".join(kept_lines).strip(), 0

    def _foreground_cron_result_text(text: str) -> str:
        return _foreground_cron_result_view(text)[0]

    def _foreground_cron_status_text(job: object, status: object) -> str:
        normalized = str(status or "").strip()
        name = str(getattr(job, "name", "") or "cron").strip()
        if normalized in {"sent", "saved"}:
            return f"Manual scheduled task run started: {name}. The result was delivered to the configured destination."
        if normalized == "silent":
            return "Cron ran successfully; no output was produced."
        if normalized == "deferred":
            return f"Manual scheduled task run started: {name}. The result will be delivered to this chat when ready."
        return ""

    def _call_cron_runner(runner: Callable[..., str | dict[str, object] | None], job: object, invocation: ToolInvocation):
        try:
            parameters = inspect.signature(runner).parameters
            accepts_invocation = (
                any(parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in parameters.values())
                or "invocation" in parameters
                or len(
                    [
                        parameter
                        for parameter in parameters.values()
                        if parameter.kind
                        in {
                            inspect.Parameter.POSITIONAL_ONLY,
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        }
                    ]
                )
                >= 2
            )
        except (TypeError, ValueError):
            accepts_invocation = False
        if accepts_invocation:
            return runner(job, invocation)
        return runner(job)

    def _foreground_cron_failure_text(reason: str) -> str:
        if reason == "cron_run_raw_tool_payload":
            return (
                "Cron run was blocked because it produced raw structured tool output "
                "instead of a deliverable report."
            )
        if reason == "cron_run_internal_tool_output_leaked":
            return "Cron run was blocked because it tried to deliver internal tool reference content."
        if reason == "cron_run_reached_iteration_limit":
            return "Cron run stopped before producing a deliverable result."
        if reason == "cron_run_waiting_for_approval":
            return "Cron run is waiting for approval."
        if reason:
            return "Cron run did not deliver its result to the configured platform."
        return ""

    def _foreground_cron_runner_output(runner_output: str | dict[str, object] | None) -> str | dict[str, object] | None:
        if not isinstance(runner_output, dict):
            return runner_output
        delivery_status = str(runner_output.get("cron_delivery_status") or "").strip()
        if delivery_status or runner_output.get("cron_delivery_failed") or runner_output.get("cron_run_failed"):
            allowed_keys = {
                "cron_delivery_status",
                "cron_delivery_failed",
                "cron_run_failed",
                "reached_iteration_limit",
                "raw_tool_payload_blocked",
                "suspended_for_approval",
                "approval_id",
                "reason",
            }
            if delivery_status == "deferred" and runner_output.get("mini_agent_dispatch"):
                allowed_keys.update({
                    "mini_agent_dispatch",
                    "task_group_id",
                    "planner_summary",
                    "text",
                    "final_text",
                    "message",
                    "result_text",
                    "status_delivered",
                })
            return {
                key: value
                for key, value in runner_output.items()
                if key in allowed_keys
            }
        sanitized = dict(runner_output)
        sanitized.pop("artifact_paths", None)
        sanitized.pop("artifacts", None)
        for key in ("text", "result_text", "message", "final_text"):
            value = sanitized.get(key)
            if isinstance(value, str):
                sanitized_value = _foreground_cron_result_text(value)
                if sanitized_value:
                    sanitized[key] = sanitized_value
                else:
                    sanitized.pop(key, None)
        return sanitized

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
                numbered_matches = _numbered_cron_matches(matches)
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output={
                        "name": raw_name,
                        "matches": numbered_matches,
                        "message": "\n".join(
                            f"{item['selection_index']}. {item['name']}" for item in numbered_matches
                        ),
                        "presentation_hint": "Ask the user to choose by number.",
                    },
                    error="Multiple crons matched; ask the user to choose by number.",
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
                    "has_task": bool(str(job.task or "").strip()),
                    "reason": "cron_runner_not_configured",
                },
                error="This runtime can list crons but cannot run them on demand.",
            )
        try:
            runner_output = _call_cron_runner(cron_runner, job, invocation)
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
                runner_failure_reason = str(runner_output.get("reason") or "cron_delivery_failed")
            elif runner_output.get("suspended_for_approval"):
                runner_failed = True
                runner_failure_reason = "cron_run_waiting_for_approval"
        jobs = load_crons()
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        from nullion.response_fulfillment_contract import guaranteed_user_visible_text

        delivery_status = runner_output.get("cron_delivery_status") if isinstance(runner_output, dict) else ""
        foreground_reply_suppressed = str(delivery_status or "").strip() in {
            "saved",
            "sent",
            "partial_success",
        }
        result_text = (
            ""
            if foreground_reply_suppressed
            else _foreground_cron_failure_text(runner_failure_reason)
            or _foreground_cron_status_text(job, delivery_status)
        )
        removed_media_count = 0
        if not result_text and not foreground_reply_suppressed:
            raw_result_text = guaranteed_user_visible_text(subject=job, output=runner_output, kind="cron")
            result_text, removed_media_count = _foreground_cron_result_view(raw_result_text)
            if not result_text:
                result_text = _foreground_cron_no_output_text()
        updated_stored_job = False
        for stored in jobs:
            if stored.id == job.id:
                stored.last_run = now
                stored.last_result = (
                    f"manual run failed: {runner_failure_reason}"
                    if runner_failed
                    else result_text
                )
                updated_stored_job = True
                break
        if updated_stored_job:
            save_crons(jobs)
        output: dict[str, object] = {
            "id": job.id,
            "name": job.name,
            "has_task": bool(str(job.task or "").strip()),
            "workspace_id": job.workspace_id,
            "last_run": now,
            "message": f"Ran cron '{job.name}' ({job.id}) now.",
            "foreground_auto_attach_created_artifacts": False,
        }
        if result_text:
            output["result_text"] = result_text
        if foreground_reply_suppressed:
            output["foreground_reply_suppressed"] = True
        if delivery_status:
            output["delivery_status"] = str(delivery_status)
            output["cron_delivery_status"] = str(delivery_status)
        if (
            isinstance(runner_output, dict)
            and str(delivery_status or "").strip() == "deferred"
            and runner_output.get("mini_agent_dispatch")
        ):
            output["mini_agent_dispatch"] = True
            task_group_id = str(runner_output.get("task_group_id") or "").strip()
            if task_group_id:
                output["task_group_id"] = task_group_id
            planner_summary = str(runner_output.get("planner_summary") or "").strip()
            if planner_summary:
                output["planner_summary"] = planner_summary
            if runner_output.get("status_delivered") is True:
                output["status_delivered"] = True
        if removed_media_count:
            output["foreground_media_directive_count"] = removed_media_count
        if isinstance(runner_output, dict):
            output["result"] = _foreground_cron_runner_output(runner_output)
        if runner_failed:
            approval_id = runner_output.get("approval_id") if isinstance(runner_output, dict) else None
            if runner_failure_reason == "cron_run_waiting_for_approval":
                output["reason"] = "approval_required"
                output["requires_approval"] = True
                if isinstance(approval_id, str) and approval_id:
                    output["approval_id"] = approval_id
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="denied",
                    output=output,
                    error="Approval required before the cron can continue.",
                )
            output["reason"] = runner_failure_reason
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=output,
                error=(
                    "Cron run did not finish cleanly; check the Doctor action or retry after cleanup."
                    if runner_failure_reason == "cron_run_reached_iteration_limit"
                    else "Cron run produced raw structured tool output instead of a deliverable report."
                    if runner_failure_reason == "cron_run_raw_tool_payload"
                    else "Cron run tried to deliver internal tool reference content."
                    if runner_failure_reason == "cron_run_internal_tool_output_leaked"
                    else "Cron run did not deliver its result to the configured platform."
                    if runner_failure_reason.startswith("cron_delivery") or runner_failure_reason.startswith("cron_run")
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
    cron_runner: Callable[..., str | dict[str, object] | None] | None = None,
    default_delivery_channel: str = "",
    default_delivery_target: str = "",
) -> None:
    """Register cron management tools into an existing ToolRegistry.

    Call this after building the registry so the agent can create, list,
    update, toggle, run, and delete scheduled cron jobs.
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
            capability_tags=("scheduler", "cron"),
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
                "enabled state, next run time, and stored task instructions in structured output. "
                "Optional args: workspace_id, include_all_workspaces."
            ),
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=10,
            capability_tags=("scheduler", "cron"),
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
            capability_tags=("scheduler", "cron"),
        ),
        _build_delete_cron_handler(),
    )
    registry.register(
        ToolSpec(
            name="update_cron",
            description=(
                "Update a scheduled cron job by id. Required args: id. "
                "Optional mutable fields: name, schedule, task, enabled, workspace_id, delivery_channel, delivery_target."
            ),
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=10,
            capability_tags=("scheduler", "cron"),
        ),
        _build_update_cron_handler(),
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
            capability_tags=("scheduler", "cron"),
        ),
        _build_toggle_cron_handler(),
    )
    registry.register(
        ToolSpec(
            name="run_cron",
            description=(
                "Run an existing scheduled cron job immediately. Use the exact visible cron name from list_crons "
                "when it is known, or pass the user's partial/descriptive reference as name so the scheduler can "
                "resolve it against the structured cron records. Use id only when names are ambiguous or the user "
                "explicitly provides an id. Required args: id or name. Matching is conservative and "
                "punctuation-insensitive; ambiguous references return numbered candidate options instead of "
                "running a job."
            ),
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=120,
            capability_tags=("scheduler", "cron"),
            input_schema={
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Exact cron id when known."},
                    "name": {
                        "type": "string",
                        "description": "Exact, partial, or descriptive cron name/reference to resolve conservatively.",
                    },
                },
                "additionalProperties": False,
            },
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
    "register_weather_forecast_tool",
    "register_web_extension",
    "register_workspace_plugin",
]

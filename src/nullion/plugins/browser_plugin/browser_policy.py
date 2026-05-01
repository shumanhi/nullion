"""Browser plugin — URL policy and SSRF guard.

No browser dependencies. Safe to import anywhere.
"""
from __future__ import annotations

import ipaddress
import os
import re
from urllib.parse import urlparse


class BrowserPolicyViolation(Exception):
    """Raised when a URL or action fails the browser policy check."""


def _parse_domain_list(raw: str) -> frozenset[str]:
    return frozenset(d.strip().lower() for d in raw.split(",") if d.strip())


def _is_private_host(host: str) -> bool:
    """Return True if host resolves to a private/loopback/link-local address."""
    lowered = host.strip().lower()
    if lowered in {"localhost", "0.0.0.0", "0"}:
        return True
    try:
        addr = ipaddress.ip_address(lowered)
        return (
            addr.is_loopback
            or addr.is_link_local
            or addr.is_private
            or addr.is_reserved
            or addr.is_unspecified
        )
    except ValueError:
        normalized_ipv4 = _normalize_ipv4_host(lowered)
        if normalized_ipv4 is not None:
            return _is_private_host(normalized_ipv4)
        # Hostname — we can't fully resolve here; block obvious internal patterns
        return lowered.endswith((".local", ".internal", ".corp", ".lan"))


def _normalize_ipv4_host(host: str) -> str | None:
    """Normalize legacy numeric IPv4 forms accepted by some resolvers."""
    atom = r"(?:0x[0-9a-f]+|0[0-7]*|[0-9]+)"
    if not re.fullmatch(rf"{atom}(?:\.{atom}){{0,3}}", host):
        return None
    values: list[int] = []
    for part in host.split("."):
        base = 10
        digits = part
        if part.startswith("0x"):
            base = 16
            digits = part[2:]
        elif len(part) > 1 and part.startswith("0"):
            base = 8
            digits = part[1:] or "0"
        try:
            values.append(int(digits, base))
        except ValueError:
            return None
    if len(values) == 1:
        value = values[0]
        if not 0 <= value <= 0xFFFFFFFF:
            return None
        return ".".join(str((value >> shift) & 0xFF) for shift in (24, 16, 8, 0))
    if any(not 0 <= value <= 255 for value in values):
        return None
    values.extend([0] * (4 - len(values)))
    return ".".join(str(value) for value in values)


class BrowserPolicy:
    """Evaluates whether a given URL is safe to navigate to.

    Reads config from environment variables at construction time so that
    tests can override with monkeypatching.

    Environment variables:
        NULLION_BROWSER_ALLOWED_DOMAINS  — comma-separated allowlist (empty = allow all)
        NULLION_BROWSER_BLOCKED_DOMAINS  — comma-separated denylist
        NULLION_BROWSER_BLOCK_PRIVATE    — "true" (default) to block private IPs / localhost
    """

    def __init__(self) -> None:
        raw_allowed = os.environ.get("NULLION_BROWSER_ALLOWED_DOMAINS", "")
        raw_blocked = os.environ.get("NULLION_BROWSER_BLOCKED_DOMAINS", "")
        self.allowed_domains: frozenset[str] = _parse_domain_list(raw_allowed)
        self.blocked_domains: frozenset[str] = _parse_domain_list(raw_blocked)
        self.block_private: bool = (
            os.environ.get("NULLION_BROWSER_BLOCK_PRIVATE", "true").lower() != "false"
        )

    # ── Public ───────────────────────────────────────────────────────────────

    def check_url(self, url: str) -> None:
        """Raise BrowserPolicyViolation if the URL should not be navigated to."""
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            raise BrowserPolicyViolation(
                f"Only http/https URLs are allowed. Got scheme: {parsed.scheme!r}"
            )

        host = (parsed.hostname or "").lower()
        if not host:
            raise BrowserPolicyViolation("URL has no host.")

        if self.block_private and _is_private_host(host):
            raise BrowserPolicyViolation(
                f"Navigation to private/local host blocked by SSRF guard: {host!r}. "
                "Set NULLION_BROWSER_BLOCK_PRIVATE=false to allow."
            )

        if host in self.blocked_domains:
            raise BrowserPolicyViolation(
                f"Domain {host!r} is blocked by NULLION_BROWSER_BLOCKED_DOMAINS."
            )

        if self.allowed_domains and host not in self.allowed_domains:
            raise BrowserPolicyViolation(
                f"Domain {host!r} is not in NULLION_BROWSER_ALLOWED_DOMAINS. "
                f"Allowed: {sorted(self.allowed_domains)}"
            )

    def is_allowed(self, url: str) -> bool:
        """Return True without raising."""
        try:
            self.check_url(url)
            return True
        except BrowserPolicyViolation:
            return False


# Module-level default instance — replace in tests via dependency injection.
_default_policy: BrowserPolicy | None = None


def get_default_policy() -> BrowserPolicy:
    global _default_policy
    if _default_policy is None:
        _default_policy = BrowserPolicy()
    return _default_policy

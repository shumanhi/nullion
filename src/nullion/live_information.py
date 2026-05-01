"""Typed live-information route classification for Nullion chat."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse


class LiveInformationRoute(str, Enum):
    NONE = "none"
    LIVE_LOOKUP = "live_lookup"


class LiveInformationResolution(str, Enum):
    NOT_REQUIRED = "not_required"
    PREFERRED_PLUGIN_PATH = "preferred_plugin_path"
    CORE_FALLBACK = "core_fallback"
    APPROVAL_REQUIRED = "approval_required"
    NO_USEFUL_RESULT = "no_useful_result"
    BLOCKED = "blocked"


_ACTIONABLE_LIVE_INFORMATION_RESOLUTIONS: tuple[LiveInformationResolution, ...] = (
    LiveInformationResolution.PREFERRED_PLUGIN_PATH,
    LiveInformationResolution.CORE_FALLBACK,
    LiveInformationResolution.APPROVAL_REQUIRED,
    LiveInformationResolution.NO_USEFUL_RESULT,
    LiveInformationResolution.BLOCKED,
)
_LIVE_INFORMATION_RESOLUTION_LABELS: dict[LiveInformationResolution, str] = {
    LiveInformationResolution.PREFERRED_PLUGIN_PATH: "preferred plugin path",
    LiveInformationResolution.CORE_FALLBACK: "core fallback path",
    LiveInformationResolution.APPROVAL_REQUIRED: "approval required",
    LiveInformationResolution.NO_USEFUL_RESULT: "no useful result",
    LiveInformationResolution.BLOCKED: "blocked",
}


@dataclass(frozen=True, slots=True)
class LiveInformationResolutionDecision:
    route: LiveInformationRoute
    resolution: LiveInformationResolution
    required_plugins: tuple[str, ...]
    missing_plugins: tuple[str, ...]


_LIVE_LOOKUP_CUE_TOKENS: frozenset[str] = frozenset(
    {
        "fetch",
        "search",
        "find",
        "lookup",
        "check",
        "open",
        "visit",
        "read",
        "curl",
        "today",
        "tomorrow",
        "current",
        "latest",
        "live",
        "price",
        "prices",
        "availability",
        "available",
        "hours",
        "schedule",
        "times",
        "nearby",
        "news",
        "headline",
        "headlines",
        "stock",
        "stocks",
        "market",
        "markets",
    }
)
_LIVE_LOOKUP_CUE_PHRASES: tuple[str, ...] = (
    "look up",
    "near me",
    "breaking news",
    "top stories",
)
_NON_LIVE_INTERNAL_PHRASES: tuple[str, ...] = (
    "your own code",
    "own code",
    "codebase",
    "repo",
    "repository",
    "source code",
    "tooling",
    "capability",
    "capabilities",
    "what tools are available",
)
_NON_LIVE_INTERNAL_TOKENS: frozenset[str] = frozenset({"tool", "tools"})
_NON_LIVE_EXECUTION_PHRASES: tuple[str, ...] = (
    "python script",
    "curl call",
    "curl command",
    "api call",
)
_NON_LIVE_EXECUTION_TOKENS: frozenset[str] = frozenset(
    {
        "script",
        "execute",
        "execution",
        "terminal",
        "shell",
        "bash",
        "command",
    }
)


def _word_tokens(message: str) -> tuple[str, ...]:
    tokens: list[str] = []
    current: list[str] = []
    for char in message:
        if char.isalnum():
            current.append(char)
            continue
        if current:
            tokens.append("".join(current))
            current = []
    if current:
        tokens.append("".join(current))
    return tuple(tokens)


def _contains_any_phrase(lowered_message: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in lowered_message for phrase in phrases)


def _contains_any_token(tokens: tuple[str, ...], candidates: frozenset[str]) -> bool:
    return any(token in candidates for token in tokens)


def _looks_like_domain(value: str) -> bool:
    if "." not in value:
        return False
    host = value.strip(".,;:!?()[]{}<>'\"").lower()
    if not host or host.startswith(".") or host.endswith("."):
        return False
    parts = host.split(".")
    if len(parts) < 2 or any(not part or not part.replace("-", "").isalnum() for part in parts):
        return False
    top_level = parts[-1]
    return len(top_level) >= 2 and top_level.isalpha()


def _looks_like_zip_code(value: str) -> bool:
    cleaned = value.strip(".,;:!?()[]{}<>'\"")
    if len(cleaned) == 5 and cleaned.isdigit():
        return True
    if len(cleaned) == 10 and cleaned[:5].isdigit() and cleaned[5] == "-" and cleaned[6:].isdigit():
        return True
    return False


def _is_url_only_prompt(stripped_message: str) -> bool:
    if not stripped_message or any(char.isspace() for char in stripped_message):
        return False
    candidate = stripped_message if "://" in stripped_message else f"https://{stripped_message}"
    parsed = urlparse(candidate)
    return _looks_like_domain(parsed.hostname or "")


def _contains_live_lookup_target(lowered_message: str, message: str) -> bool:
    if "http://" in lowered_message or "https://" in lowered_message:
        return True
    parts = tuple(part for part in message.split() if part)
    return any(_looks_like_domain(part) or _looks_like_zip_code(part) for part in parts)


def _contains_live_lookup_cue(lowered_message: str, tokens: tuple[str, ...]) -> bool:
    return _contains_any_phrase(lowered_message, _LIVE_LOOKUP_CUE_PHRASES) or _contains_any_token(
        tokens,
        _LIVE_LOOKUP_CUE_TOKENS,
    )


def _contains_non_live_internal_signal(lowered_message: str, tokens: tuple[str, ...]) -> bool:
    return _contains_any_phrase(lowered_message, _NON_LIVE_INTERNAL_PHRASES) or _contains_any_token(
        tokens,
        _NON_LIVE_INTERNAL_TOKENS,
    )


def _contains_non_live_execution_signal(lowered_message: str, tokens: tuple[str, ...]) -> bool:
    return _contains_any_phrase(lowered_message, _NON_LIVE_EXECUTION_PHRASES) or _contains_any_token(
        tokens,
        _NON_LIVE_EXECUTION_TOKENS,
    )


def classify_live_information_route(message: str) -> LiveInformationRoute:
    stripped = message.strip()
    lowered_message = message.lower()
    tokens = _word_tokens(lowered_message)
    if _contains_non_live_internal_signal(lowered_message, tokens):
        return LiveInformationRoute.NONE
    if _is_url_only_prompt(stripped):
        return LiveInformationRoute.LIVE_LOOKUP
    if _contains_live_lookup_cue(lowered_message, tokens) and _contains_live_lookup_target(
        lowered_message,
        message,
    ):
        return LiveInformationRoute.LIVE_LOOKUP
    if _contains_live_lookup_cue(lowered_message, tokens):
        return LiveInformationRoute.LIVE_LOOKUP
    if _contains_non_live_execution_signal(lowered_message, tokens):
        return LiveInformationRoute.NONE
    return LiveInformationRoute.NONE


def route_required_plugins(route: LiveInformationRoute) -> tuple[str, ...]:
    if route is LiveInformationRoute.LIVE_LOOKUP:
        return ("search_plugin",)
    return ()


def actionable_live_information_resolutions() -> tuple[LiveInformationResolution, ...]:
    return _ACTIONABLE_LIVE_INFORMATION_RESOLUTIONS


def format_live_information_resolution_label(resolution: LiveInformationResolution | str) -> str | None:
    if isinstance(resolution, str):
        try:
            normalized_resolution = LiveInformationResolution(resolution)
        except ValueError:
            return None
    else:
        normalized_resolution = resolution
    return _LIVE_INFORMATION_RESOLUTION_LABELS.get(normalized_resolution)


def format_live_information_states_for_prompt() -> str:
    labels = [
        label
        for label in (
            format_live_information_resolution_label(resolution)
            for resolution in actionable_live_information_resolutions()
        )
        if isinstance(label, str)
    ]
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    return f"{', '.join(labels[:-1])}, or {labels[-1]}"


def resolve_live_information_resolution(
    route: LiveInformationRoute,
    *,
    tool_registry,
    fallback_available: bool,
) -> LiveInformationResolutionDecision:
    required_plugins = route_required_plugins(route)
    if route is LiveInformationRoute.NONE or not required_plugins:
        return LiveInformationResolutionDecision(
            route=route,
            resolution=LiveInformationResolution.NOT_REQUIRED,
            required_plugins=required_plugins,
            missing_plugins=(),
        )

    missing_plugins = tuple(
        plugin_name
        for plugin_name in required_plugins
        if tool_registry is None or not tool_registry.is_plugin_installed(plugin_name)
    )
    if not missing_plugins:
        resolution = LiveInformationResolution.PREFERRED_PLUGIN_PATH
    elif fallback_available:
        resolution = LiveInformationResolution.CORE_FALLBACK
    else:
        resolution = LiveInformationResolution.BLOCKED

    return LiveInformationResolutionDecision(
        route=route,
        resolution=resolution,
        required_plugins=required_plugins,
        missing_plugins=missing_plugins,
    )


__all__ = [
    "LiveInformationResolution",
    "LiveInformationRoute",
    "LiveInformationResolutionDecision",
    "actionable_live_information_resolutions",
    "classify_live_information_route",
    "format_live_information_resolution_label",
    "format_live_information_states_for_prompt",
    "resolve_live_information_resolution",
    "route_required_plugins",
]

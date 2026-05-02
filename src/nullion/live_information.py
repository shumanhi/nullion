"""Typed live-information route classification for Nullion chat."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from nullion.task_frames import extract_url_target


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


def classify_live_information_route(message: str) -> LiveInformationRoute:
    if extract_url_target(message) is not None:
        return LiveInformationRoute.LIVE_LOOKUP
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

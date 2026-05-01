"""Service manager helpers for restarting installed Nullion services."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal, TypedDict

from langgraph.graph import END, START, StateGraph


ServiceGroup = Literal["core", "chat", "support"]
ServiceManager = Literal["auto", "launchd", "systemd", "windows"]


@dataclass(frozen=True)
class ManagedService:
    name: str
    display_name: str
    group: ServiceGroup
    launchd_labels: tuple[str, ...] = ()
    systemd_units: tuple[str, ...] = ()
    windows_tasks: tuple[str, ...] = ()
    command_hint: str | None = None


@dataclass(frozen=True)
class ServiceRestartResult:
    service: str
    ok: bool
    message: str


MANAGED_SERVICES: tuple[ManagedService, ...] = (
    ManagedService(
        name="web",
        display_name="Web",
        group="core",
        launchd_labels=("com.nullion.web",),
        systemd_units=("nullion.service",),
        windows_tasks=("Nullion Web Dashboard",),
        command_hint="nullion-web",
    ),
    ManagedService(
        name="tray",
        display_name="Tray",
        group="core",
        launchd_labels=("com.nullion.tray",),
        systemd_units=("nullion-tray.service",),
        windows_tasks=("Nullion Tray",),
        command_hint="nullion-tray",
    ),
    ManagedService(
        name="slack",
        display_name="Slack",
        group="chat",
        launchd_labels=("ai.nullion.slack", "com.nullion.slack"),
        systemd_units=("nullion-slack.service",),
        windows_tasks=("Nullion Slack",),
        command_hint="nullion-slack",
    ),
    ManagedService(
        name="discord",
        display_name="Discord",
        group="chat",
        launchd_labels=("ai.nullion.discord", "com.nullion.discord"),
        systemd_units=("nullion-discord.service",),
        windows_tasks=("Nullion Discord",),
        command_hint="nullion-discord",
    ),
    ManagedService(
        name="recovery",
        display_name="Recovery",
        group="support",
        launchd_labels=("ai.nullion.recovery",),
        systemd_units=("nullion-recovery.service",),
        windows_tasks=("Nullion Recovery",),
        command_hint="nullion-recovery",
    ),
    # Restart chat ingress last so a /restart command sent from Telegram/Slack/
    # Discord has the best chance to finish any acknowledgement before its own
    # adapter is replaced.
    ManagedService(
        name="telegram",
        display_name="Telegram",
        group="chat",
        launchd_labels=("ai.nullion.telegram", "com.nullion.telegram"),
        systemd_units=("nullion-telegram.service",),
        windows_tasks=("Nullion Telegram",),
        command_hint="nullion-telegram",
    ),
)


def service_names(*, groups: set[ServiceGroup] | None = None) -> list[str]:
    if groups == {"chat"}:
        return ["telegram", "slack", "discord"]
    return [service.name for service in MANAGED_SERVICES if groups is None or service.group in groups]


def service_for_name(name: str) -> ManagedService:
    normalized = name.strip().lower()
    for service in MANAGED_SERVICES:
        if service.name == normalized:
            return service
    raise ValueError(f"Unknown service: {name}")


def launchd_plist_for_label(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"


def systemd_unit_path(unit: str) -> Path:
    return Path.home() / ".config" / "systemd" / "user" / unit


def launchd_available() -> bool:
    return sys.platform == "darwin" and shutil.which("launchctl") is not None


def systemd_available() -> bool:
    return sys.platform.startswith("linux")


def windows_tasks_available() -> bool:
    return os.name == "nt" and shutil.which("schtasks") is not None


def _run(args: list[str], *, timeout: float = 15.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=False, timeout=timeout)


def _launchd_service_running(label: str) -> bool:
    uid = os.getuid()
    target = f"gui/{uid}/{label}"
    printed = _run(["launchctl", "print", target], timeout=10)
    return printed.returncode == 0 and "state = running" in (printed.stdout or "")


def _restart_launchd_service(service: ManagedService) -> str | None:
    uid = os.getuid()
    for label in service.launchd_labels:
        plist = launchd_plist_for_label(label)
        if not plist.exists():
            continue
        target = f"gui/{uid}/{label}"
        listed = _run(["launchctl", "list", label], timeout=10)
        if listed.returncode != 0:
            _run(["launchctl", "bootstrap", f"gui/{uid}", str(plist)], timeout=15)
        restarted = _run(["launchctl", "kickstart", "-k", target], timeout=15)
        if restarted.returncode == 0:
            return f"Restarted the {service.display_name} service ({label})."
        if _launchd_service_running(label):
            return f"Restarted the {service.display_name} service ({label})."
        error = (restarted.stderr or restarted.stdout or "launchctl kickstart failed").strip()
        raise RuntimeError(f"{service.display_name} launchd restart failed for {label}: {error}")
    return None


def _restart_systemd_service(service: ManagedService) -> str | None:
    for unit in service.systemd_units:
        result = _run(["systemctl", "--user", "restart", unit], timeout=20)
        if result.returncode == 0:
            return f"Restarted the {service.display_name} service ({unit})."
        error = (result.stderr or result.stdout or "systemctl restart failed").strip()
        lowered = error.lower()
        if (
            not systemd_unit_path(unit).exists()
            and (
                "not found" in lowered
                or "could not be found" in lowered
                or "does not exist" in lowered
                or "not loaded" in lowered
            )
        ):
            continue
        raise RuntimeError(f"{service.display_name} systemd restart failed for {unit}: {error}")
    return None


def _restart_windows_service(service: ManagedService) -> str | None:
    for task in service.windows_tasks:
        query = _run(["schtasks", "/Query", "/TN", task], timeout=10)
        if query.returncode != 0:
            continue
        _run(["schtasks", "/End", "/TN", task], timeout=10)
        started = _run(["schtasks", "/Run", "/TN", task], timeout=10)
        if started.returncode == 0:
            return f"Restarted the {service.display_name} service ({task})."
        error = (started.stderr or started.stdout or "schtasks /Run failed").strip()
        raise RuntimeError(f"{service.display_name} Windows task restart failed for {task}: {error}")
    return None


def restart_managed_service(name: str, *, manager: ServiceManager = "auto") -> str:
    service = service_for_name(name)
    message: str | None = None
    if manager == "launchd" or (manager == "auto" and launchd_available()):
        message = _restart_launchd_service(service)
    elif manager == "systemd" or (manager == "auto" and systemd_available()):
        message = _restart_systemd_service(service)
    elif manager == "windows" or (manager == "auto" and windows_tasks_available()):
        message = _restart_windows_service(service)
    else:
        raise RuntimeError("No supported service manager is available.")
    if message:
        return message
    raise FileNotFoundError(f"No managed service is installed for {service.display_name}.")


class _ManagedServicesRestartState(TypedDict, total=False):
    names: list[str]
    groups: set[ServiceGroup] | None
    continue_on_error: bool
    manager: ServiceManager
    index: int
    results: list[ServiceRestartResult]


def _managed_services_restart_prepare_node(state: _ManagedServicesRestartState) -> dict[str, object]:
    names = state.get("names")
    groups = state.get("groups")
    targets = list(names) if names is not None else service_names(groups=groups)
    return {"names": targets, "index": 0, "results": []}


def _managed_services_restart_route_next(state: _ManagedServicesRestartState) -> str:
    return "restart_one" if int(state.get("index") or 0) < len(state.get("names") or []) else END


def _managed_services_restart_one_node(state: _ManagedServicesRestartState) -> dict[str, object]:
    names = list(state.get("names") or [])
    index = int(state.get("index") or 0)
    results = list(state.get("results") or [])
    if index >= len(names):
        return {"index": index, "results": results}
    name = names[index]
    manager = state.get("manager") or "auto"
    continue_on_error = bool(state.get("continue_on_error", True))
    try:
        message = restart_managed_service(name, manager=manager)
    except FileNotFoundError as exc:
        if not continue_on_error:
            raise
        results.append(ServiceRestartResult(service=name, ok=False, message=str(exc)))
    except Exception as exc:
        if not continue_on_error:
            raise
        results.append(ServiceRestartResult(service=name, ok=False, message=str(exc)))
    else:
        results.append(ServiceRestartResult(service=name, ok=True, message=message))
    return {"index": index + 1, "results": results}


@lru_cache(maxsize=1)
def _compiled_managed_services_restart_graph():
    graph = StateGraph(_ManagedServicesRestartState)
    graph.add_node("prepare", _managed_services_restart_prepare_node)
    graph.add_node("restart_one", _managed_services_restart_one_node)
    graph.add_edge(START, "prepare")
    graph.add_conditional_edges("prepare", _managed_services_restart_route_next, {"restart_one": "restart_one", END: END})
    graph.add_conditional_edges("restart_one", _managed_services_restart_route_next, {"restart_one": "restart_one", END: END})
    return graph.compile()


def restart_managed_services(
    names: list[str] | tuple[str, ...] | None = None,
    *,
    groups: set[ServiceGroup] | None = None,
    continue_on_error: bool = True,
    manager: ServiceManager = "auto",
) -> list[ServiceRestartResult]:
    final_state = _compiled_managed_services_restart_graph().invoke(
        {
            "names": list(names) if names is not None else None,
            "groups": groups,
            "continue_on_error": continue_on_error,
            "manager": manager,
        },
        config={"configurable": {"thread_id": "managed-services-restart"}},
    )
    return list(final_state.get("results") or [])


def successful_restart_message(results: list[ServiceRestartResult]) -> str:
    return " ".join(result.message for result in results if result.ok)


__all__ = [
    "MANAGED_SERVICES",
    "ManagedService",
    "ServiceRestartResult",
    "ServiceManager",
    "launchd_available",
    "launchd_plist_for_label",
    "restart_managed_service",
    "restart_managed_services",
    "service_for_name",
    "service_names",
    "successful_restart_message",
    "systemd_available",
    "systemd_unit_path",
    "windows_tasks_available",
]

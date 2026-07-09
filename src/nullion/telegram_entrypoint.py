"""Executable Telegram app entrypoint for Nullion."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import time
import inspect
import threading
from typing import Callable

from nullion.config import ProviderBinding, load_env_file_into_environ, load_settings
from nullion.entrypoint_guard import run_single_instance_entrypoint, run_user_facing_entrypoint
from nullion.artifacts import ensure_artifact_root
from nullion.messaging_adapters import (
    messaging_file_allowed_roots,
    principal_id_for_messaging_identity,
    retry_messaging_delivery_operation,
)
from nullion.providers import resolve_plugin_provider_kwargs
from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_persistence import (
    list_runtime_store_backups,
    restore_runtime_store_backup,
    restore_sqlite_auxiliary_rows_from_candidates,
)
from nullion.telegram_app import build_messaging_operator_service, build_telegram_operator_service
from nullion.telegram_transport import build_telegram_bot
from nullion.tools import (
    SandboxLauncherTerminalExecutorBackend,
    SubprocessTerminalExecutorBackend,
    TerminalAttestationVerifier,
    create_plugin_tool_registry,
    register_calendar_plugin,
    register_email_plugin,
    register_cron_tools,
    register_media_plugin,
    register_reminder_tools,
    register_search_plugin,
    register_workspace_plugin,
    verify_terminal_backend_attestation,
)
from nullion.web_research_policy import (
    default_browser_backend_for_web_research,
    direct_web_fetch_enabled,
    preferred_browser_backend_from_env,
    should_register_search_plugin,
)


logger = logging.getLogger(__name__)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_ENV_PATH = Path.home() / ".nullion" / ".env"
_DEFAULT_CHECKPOINT_PATH = Path.home() / ".nullion" / "runtime.db"
_MANUAL_CRON_DOCTOR_MONITOR_SECONDS_ENV = "NULLION_MANUAL_CRON_DOCTOR_MONITOR_SECONDS"
_DEFAULT_MANUAL_CRON_DOCTOR_MONITOR_SECONDS = 900.0


def _manual_cron_doctor_monitor_seconds() -> float:
    raw_value = os.environ.get(_MANUAL_CRON_DOCTOR_MONITOR_SECONDS_ENV, "").strip()
    if not raw_value:
        return _DEFAULT_MANUAL_CRON_DOCTOR_MONITOR_SECONDS
    try:
        parsed = float(raw_value)
    except ValueError:
        logger.warning(
            "Ignoring invalid %s=%r",
            _MANUAL_CRON_DOCTOR_MONITOR_SECONDS_ENV,
            raw_value,
        )
        return _DEFAULT_MANUAL_CRON_DOCTOR_MONITOR_SECONDS
    return max(0.0, parsed)


def _manual_cron_monitoring_text(job, *, elapsed_seconds: float) -> str:
    name = str(getattr(job, "name", "") or "scheduled task").strip()
    elapsed = int(max(0, elapsed_seconds))
    return (
        "Doctor is monitoring this manual scheduled task because it is still running "
        f"after {elapsed} seconds. It has not been stopped; Nullion will keep working "
        "and will deliver the result when the run finishes."
        f"\n\nTask: {name}"
    )


def _report_manual_cron_monitoring(
    runtime,
    *,
    job,
    invocation,
    channel: str,
    target: str,
    elapsed_seconds: float,
) -> None:
    from nullion.health import HealthIssueType

    runtime.report_health_issue(
        issue_type=HealthIssueType.STALLED,
        source="manual_cron",
        message="A manual scheduled task is still running",
        details={
            "recommendation_code": "monitor_manual_cron",
            "cron_id": str(getattr(job, "id", "") or ""),
            "cron_name": str(getattr(job, "name", "") or ""),
            "invocation_id": str(getattr(invocation, "invocation_id", "") or ""),
            "principal_id": str(getattr(invocation, "principal_id", "") or ""),
            "delivery_channel": str(channel or ""),
            "delivery_target": str(target or ""),
            "elapsed_seconds": int(max(0, elapsed_seconds)),
        },
    )


def _run_async_sync(factory) -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return bool(asyncio.run(factory()))
    result: list[object] = []
    errors: list[BaseException] = []

    def _runner() -> None:
        try:
            result.append(asyncio.run(factory()))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=_runner, name="nullion-telegram-cron-delivery", daemon=True)
    thread.start()
    thread.join()
    if errors:
        raise errors[0]
    return bool(result[0]) if result else False


def _resolve_browser_backend() -> str | None:
    if os.environ.get("NULLION_BROWSER_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}:
        return None
    try:
        from nullion.auth import load_stored_credentials
        creds = load_stored_credentials() or {}
        be = str(creds.get("browser_backend", "")).strip().lower()
        if be:
            return be
    except Exception:
        logger.debug("Unable to read stored browser backend credentials.", exc_info=True)
    be = os.environ.get("NULLION_BROWSER_BACKEND", "").strip().lower()
    if be:
        return be
    preferred_backend = preferred_browser_backend_from_env()
    if preferred_backend:
        return preferred_backend
    plugins_env = os.environ.get("NULLION_PLUGINS", "")
    if any(p.strip().lower() == "browser" for p in plugins_env.split(",")):
        return "auto"
    return None


def _is_transient_telegram_polling_timeout(error: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if current.__class__.__name__ in {"TimedOut", "ReadTimeout"}:
            return True
        current = current.__cause__ or current.__context__
    return False


def _checkpoint_is_valid_sqlite(checkpoint: Path) -> bool:
    if checkpoint.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
        return False
    try:
        with sqlite3.connect(str(checkpoint), timeout=10) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
    except sqlite3.Error:
        return False
    return bool(row and row[0] == "ok")


def _recover_runtime_from_corrupt_checkpoint(checkpoint: Path, error: Exception):
    if not checkpoint.exists():
        raise error

    if _checkpoint_is_valid_sqlite(checkpoint):
        logger.error(
            "Runtime checkpoint load failed, but SQLite integrity check passed; "
            "leaving checkpoint in place for row-level repair (checkpoint=%s, error=%s)",
            checkpoint,
            error,
        )
        raise error

    suffix = int(time.time() * 1000)
    candidate = checkpoint.with_name(f"{checkpoint.name}.corrupt-{suffix}")
    collision = 0
    while candidate.exists():
        collision += 1
        candidate = checkpoint.with_name(f"{checkpoint.name}.corrupt-{suffix}-{collision}")
    checkpoint.replace(candidate)
    logger.warning(
        "Moved corrupt runtime checkpoint aside (checkpoint=%s, corrupt_backup=%s, error=%s)",
        checkpoint,
        candidate,
        error,
    )
    for backup in list_runtime_store_backups(checkpoint):
        if backup.get("kind") != "backup" or not backup.get("restorable"):
            continue
        restore_id = str(backup.get("restore_id", ""))
        try:
            restore_runtime_store_backup(checkpoint, generation=restore_id)
        except Exception as restore_error:
            logger.warning(
                "Skipping runtime checkpoint backup during recovery "
                "(checkpoint=%s, backup=%s, error=%s)",
                checkpoint,
                backup.get("name"),
                restore_error,
            )
            continue
        logger.warning(
            "Recovered runtime checkpoint from backup "
            "(checkpoint=%s, corrupt_backup=%s, restored_backup=%s)",
            checkpoint,
            candidate,
            backup.get("name"),
        )
        restore_sqlite_auxiliary_rows_from_candidates((candidate, checkpoint), checkpoint)
        return bootstrap_persistent_runtime(checkpoint)
    logger.warning(
        "No restorable runtime checkpoint backup found; starting with a new runtime "
        "(checkpoint=%s, corrupt_backup=%s)",
        checkpoint,
        candidate,
    )
    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.checkpoint()
    restore_sqlite_auxiliary_rows_from_candidates((candidate,), checkpoint)
    return runtime



_REQUIRED_LAUNCHER_ATTESTED_CAPABILITIES = (
    "network_policy_enforced",
    "network_policy_enforced.none",
    "network_policy_enforced.localhost_only",
    "network_policy_enforced.approved_only",
    "approved_only_enforced_via_local_allowlist_proxy",
)


def _resolve_launcher_command(launcher_command: str) -> str | None:
    normalized_launcher_command = launcher_command.strip()
    command_path = Path(normalized_launcher_command)
    if command_path.parent == Path():
        if shutil.which(normalized_launcher_command) is None:
            return None
        return normalized_launcher_command
    candidates = [command_path]
    if not command_path.is_absolute():
        candidates.append(_REPO_ROOT / command_path)
    for candidate in candidates:
        if candidate.exists():
            resolved_candidate = candidate.resolve()
            if not resolved_candidate.is_file() or not resolved_candidate.stat().st_mode & 0o111:
                raise ValueError(f"terminal launcher command is not executable: {resolved_candidate}")
            return str(resolved_candidate)
    return None



def _terminal_executor_backend_from_settings(
    settings,
    *,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
    launcher_backend_factory=SandboxLauncherTerminalExecutorBackend,
):
    terminal_settings = settings.terminal_execution
    if terminal_settings.backend_mode == "launcher":
        launcher_command = terminal_settings.launcher_command
        if not isinstance(launcher_command, str) or not launcher_command.strip():
            raise ValueError("terminal launcher mode requires a launcher command")
        normalized_launcher_command = launcher_command.strip()
        resolved_launcher_command = _resolve_launcher_command(normalized_launcher_command)
        if resolved_launcher_command is None:
            raise ValueError(f"terminal launcher command not found: {normalized_launcher_command}")
        backend = launcher_backend_factory(
            launcher_command=resolved_launcher_command,
            launcher_args=terminal_settings.launcher_args,
        )
        descriptor = backend.describe()
        missing_capabilities = tuple(
            capability
            for capability in _REQUIRED_LAUNCHER_ATTESTED_CAPABILITIES
            if capability not in descriptor.attested_capabilities
        )
        if descriptor.mode != "launcher" or missing_capabilities:
            missing_labels = [*missing_capabilities]
            if descriptor.mode != "launcher":
                missing_labels.append("mode:launcher")
            raise ValueError(
                "terminal launcher mode requires attested backend capabilities: "
                + ", ".join(sorted(missing_labels))
            )
        verification = verify_terminal_backend_attestation(
            descriptor,
            required_capabilities=_REQUIRED_LAUNCHER_ATTESTED_CAPABILITIES,
            verifier=terminal_attestation_verifier,
        )
        if not verification.is_valid:
            failure_reason = verification.failure_reason or "verification failed"
            raise ValueError(
                "terminal launcher mode requires verified backend attestation: "
                + failure_reason
            )
        return backend
    if terminal_settings.backend_mode == "subprocess":
        return SubprocessTerminalExecutorBackend()
    raise ValueError(f"unknown terminal backend mode: {terminal_settings.backend_mode}")



@dataclass(frozen=True)
class PluginBootstrapSpec:
    plugin_name: str
    requires_provider_binding: bool
    registrar: object
    settings_kwargs_builder: object | None = None


_PLUGIN_BOOTSTRAP_SPECS = {
    "search_plugin": PluginBootstrapSpec(
        plugin_name="search_plugin",
        requires_provider_binding=True,
        registrar=register_search_plugin,
    ),
    "workspace_plugin": PluginBootstrapSpec(
        plugin_name="workspace_plugin",
        requires_provider_binding=False,
        registrar=register_workspace_plugin,
        settings_kwargs_builder=lambda settings, **kwargs: _workspace_plugin_kwargs_from_settings(settings, **kwargs),
    ),
    "email_plugin": PluginBootstrapSpec(
        plugin_name="email_plugin",
        requires_provider_binding=True,
        registrar=register_email_plugin,
    ),
    "calendar_plugin": PluginBootstrapSpec(
        plugin_name="calendar_plugin",
        requires_provider_binding=True,
        registrar=register_calendar_plugin,
    ),
    "media_plugin": PluginBootstrapSpec(
        plugin_name="media_plugin",
        requires_provider_binding=True,
        registrar=register_media_plugin,
    ),
}


def _workspace_plugin_kwargs_from_settings(
    settings,
    *,
    default_workspace_root: str | Path | None = None,
) -> dict[str, object]:
    def normalize_root(raw_root: object) -> str:
        if not isinstance(raw_root, str) or not raw_root:
            raise ValueError("workspace plugin roots must be absolute paths")
        candidate = Path(raw_root).expanduser()
        if not candidate.is_absolute():
            raise ValueError("workspace plugin roots must be absolute paths")
        resolved = candidate.resolve()
        if resolved == Path("/"):
            raise ValueError("workspace plugin roots must not be filesystem root")
        if not resolved.exists():
            raise ValueError("workspace plugin roots must exist")
        if not resolved.is_dir():
            raise ValueError("workspace plugin roots must be directories")
        return str(resolved)

    kwargs: dict[str, object] = {}
    if isinstance(settings.workspace_root, str) and settings.workspace_root:
        kwargs["workspace_root"] = normalize_root(settings.workspace_root)
    if getattr(settings, "allowed_roots", ()):
        kwargs["allowed_roots"] = tuple(normalize_root(root) for root in settings.allowed_roots)
    if not kwargs and default_workspace_root is not None:
        kwargs["workspace_root"] = normalize_root(str(default_workspace_root))
    if not kwargs:
        raise ValueError("workspace_plugin requires workspace_root or allowed_roots")
    return kwargs


def _provider_binding_map(settings) -> dict[str, str]:
    bindings: dict[str, str] = {}
    for binding in settings.provider_bindings:
        if not isinstance(binding, ProviderBinding):
            raise ValueError(f"invalid provider binding object: {binding!r}")
        bindings[binding.capability] = binding.provider
    return bindings



def _validate_plugin_bootstrap_settings(settings) -> dict[str, str]:
    enabled_plugins = tuple(
        plugin
        for plugin in settings.enabled_plugins
        if plugin not in {"browser", "browser_plugin"}
    )
    binding_map = _provider_binding_map(settings)
    supported_plugins = frozenset(_PLUGIN_BOOTSTRAP_SPECS)
    for plugin_name in enabled_plugins:
        if plugin_name not in supported_plugins:
            raise ValueError(f"unsupported enabled plugin: {plugin_name}")
    for capability in binding_map:
        if capability not in enabled_plugins:
            raise ValueError(f"provider binding declared for disabled plugin: {capability}")
    return binding_map



def _register_enabled_plugins(
    *,
    registry,
    enabled_plugins: tuple[str, ...],
    provider_bindings: dict[str, str],
    settings,
    workspace_plugin_default_root: str | Path | None = None,
) -> None:
    seen_plugins: set[str] = set()
    for plugin_name in enabled_plugins:
        if plugin_name in {"browser", "browser_plugin"}:
            continue
        if plugin_name == "search_plugin" and not should_register_search_plugin(settings):
            continue
        if plugin_name in seen_plugins:
            continue
        seen_plugins.add(plugin_name)
        spec = _PLUGIN_BOOTSTRAP_SPECS[plugin_name]
        registrar_kwargs: dict[str, object] = {}
        if spec.settings_kwargs_builder is not None:
            if plugin_name == "workspace_plugin":
                registrar_kwargs.update(
                    spec.settings_kwargs_builder(
                        settings,
                        default_workspace_root=workspace_plugin_default_root,
                    )
                )
            else:
                registrar_kwargs.update(spec.settings_kwargs_builder(settings))
        if spec.requires_provider_binding:
            provider_name = provider_bindings.get(plugin_name)
            if provider_name is None:
                raise ValueError(f"{plugin_name} requires provider binding")
            registrar_kwargs.update(
                resolve_plugin_provider_kwargs(
                    plugin_name=plugin_name,
                    provider_name=provider_name,
                )
            )
        spec.registrar(registry, **registrar_kwargs)



def _build_runtime_service_from_settings(
    *,
    checkpoint_path: str | Path,
    env_path: str | Path | None = None,
    service_factory=build_telegram_operator_service,
):
    checkpoint = Path(checkpoint_path)
    os.environ["NULLION_CHECKPOINT_PATH"] = str(checkpoint.expanduser())
    os.environ.setdefault("NULLION_HOME", str(checkpoint.expanduser().parent))
    if env_path is not None:
        os.environ["NULLION_ENV_FILE"] = str(Path(env_path).expanduser())
        load_env_file_into_environ(env_path, override=True)
    settings = load_settings(env_path=env_path)
    try:
        runtime = bootstrap_persistent_runtime(checkpoint)
    except (ValueError, sqlite3.Error) as exc:
        runtime = _recover_runtime_from_corrupt_checkpoint(checkpoint, exc)
    try:
        from nullion.cron_delivery import record_interrupted_cron_delivery_runs

        interrupted = record_interrupted_cron_delivery_runs(runtime.store, actor="telegram_startup")
        if interrupted:
            runtime.checkpoint()
            logger.info("Marked %d interrupted cron delivery run(s) after Telegram startup.", interrupted)
    except Exception:
        logger.debug("Could not reconcile interrupted cron delivery runs on Telegram startup.", exc_info=True)
    workspace_root = (
        Path(settings.workspace_root).expanduser()
        if isinstance(settings.workspace_root, str) and settings.workspace_root.strip()
        else checkpoint.parent
    )
    artifact_root = ensure_artifact_root(runtime)
    registry_factory_kwargs = {
        "terminal_executor_backend": _terminal_executor_backend_from_settings(settings),
        "workspace_root": workspace_root,
        "direct_web_fetch_enabled": direct_web_fetch_enabled(settings),
    }
    allowed_roots = messaging_file_allowed_roots(
        workspace_root,
        artifact_root,
        *(
            Path(root).expanduser()
            for root in settings.allowed_roots
            if isinstance(root, str) and root.strip()
        ),
    )
    registry_factory_kwargs["allowed_roots"] = allowed_roots

    registry_factory = inspect.signature(create_plugin_tool_registry)
    accepted_parameters = set(registry_factory.parameters)
    filtered_factory_kwargs = {
        key: value
        for key, value in registry_factory_kwargs.items()
        if key in accepted_parameters
    }
    active_tool_registry = create_plugin_tool_registry(**filtered_factory_kwargs)
    provider_bindings = _validate_plugin_bootstrap_settings(settings)
    enabled_plugins = list(dict.fromkeys(settings.enabled_plugins))
    try:
        from nullion.connections import infer_email_plugin_provider

        inferred_email_provider = infer_email_plugin_provider()
    except Exception:
        inferred_email_provider = None
    if (
        inferred_email_provider
        and "email_plugin" not in enabled_plugins
        and "email_plugin" not in provider_bindings
    ):
        enabled_plugins.append("email_plugin")
        provider_bindings["email_plugin"] = inferred_email_provider
    _register_enabled_plugins(
        registry=active_tool_registry,
        enabled_plugins=tuple(enabled_plugins),
        provider_bindings=provider_bindings,
        settings=settings,
        workspace_plugin_default_root=_REPO_ROOT,
    )
    if "media_plugin" not in settings.enabled_plugins:
        register_media_plugin(
            active_tool_registry,
            **resolve_plugin_provider_kwargs(
                plugin_name="media_plugin",
                provider_name="local_media_provider",
            ),
        )
    browser_backend = _resolve_browser_backend() or default_browser_backend_for_web_research(settings)
    if browser_backend:
        try:
            os.environ["NULLION_BROWSER_BACKEND"] = browser_backend
            from nullion.plugins.browser_plugin import register_browser_tools
            register_browser_tools(active_tool_registry)
            logger.info("Browser plugin registered for Telegram (backend=%s)", browser_backend)
        except Exception:
            logger.warning("Could not register browser plugin for Telegram.", exc_info=True)
    # Wire in reminder tools — they need a live runtime reference so are registered
    # separately after the registry is fully built.
    _operator_chat_id = (
        settings.telegram.operator_chat_id.strip()
        if isinstance(settings.telegram.operator_chat_id, str)
        and settings.telegram.operator_chat_id.strip()
        else None
    )
    register_reminder_tools(
        active_tool_registry,
        runtime,
        default_chat_id=_operator_chat_id,
    )
    runtime.active_tool_registry = active_tool_registry
    service = service_factory(runtime, settings=settings)

    def _record_cron_delivery_event(event_type: str, job, channel: str, target: str, conv_id: str, **extra) -> None:
        try:
            from nullion.audit import make_audit_record
            from nullion.events import make_event

            payload = {
                "cron_id": getattr(job, "id", ""),
                "cron_name": getattr(job, "name", ""),
                "workspace_id": getattr(job, "workspace_id", "workspace_admin"),
                "delivery_channel": channel,
                "delivery_target": target,
                "conversation_id": conv_id,
            }
            payload.update(extra)
            runtime.store.add_event(make_event(event_type, "cron_scheduler", payload))
            runtime.store.add_audit_record(make_audit_record(event_type, "cron_scheduler", payload))
            runtime.checkpoint()
        except Exception:
            logger.debug("Could not record Telegram cron delivery event", exc_info=True)

    def _record_cron_delivery_chat_turn(
        job,
        conversation_id: str,
        delivery_channel: str,
        delivery_target: str,
        delivered_text: str,
    ) -> None:
        try:
            from nullion.cron_delivery import record_cron_delivery_chat_turn

            record_cron_delivery_chat_turn(
                runtime.store,
                job,
                conversation_id=conversation_id,
                delivery_channel=delivery_channel,
                delivery_target=delivery_target,
                delivered_text=delivered_text,
            )
            runtime.checkpoint()
        except Exception:
            logger.debug("Could not record Telegram cron delivery chat turn", exc_info=True)

    def _refresh_service_live_config() -> None:
        refresh = getattr(service, "refresh_live_configuration", None)
        if callable(refresh):
            try:
                refresh()
            except Exception:
                logger.debug("Could not refresh live Telegram service config", exc_info=True)

    def _current_service_settings():
        _refresh_service_live_config()
        return getattr(service, "settings", settings)

    def _effective_cron_delivery_channel(job) -> str:
        from nullion.cron_delivery import effective_cron_delivery_channel

        return effective_cron_delivery_channel(job, settings=_current_service_settings())

    def _cron_delivery_target(job, channel: str) -> str:
        from nullion.cron_delivery import cron_delivery_target

        return cron_delivery_target(job, channel, settings=_current_service_settings())

    def _notify_cron_approval_required(job, channel: str, target: str, result: dict) -> None:
        _ = (job, channel, target)
        approval_id = str((result or {}).get("approval_id") or "").strip()
        if not approval_id:
            return
        try:
            from nullion.workspace_notifications import broadcast_pending_approval

            broadcast_pending_approval(
                runtime,
                approval_id,
                settings=_current_service_settings(),
                include_origin=True,
            )
        except Exception:
            logger.debug("Could not deliver manual cron approval request", exc_info=True)

    def _run_cron_agent_turn(
        job,
        conv_id: str,
        *,
        label: str,
        cancellation_checker=None,
    ) -> dict:
        from nullion.cron_delivery import run_single_agent_cron_turn

        orchestrator = getattr(service, "agent_orchestrator", None)
        return run_single_agent_cron_turn(
            job,
            conv_id,
            label=label,
            orchestrator=orchestrator,
            runtime=runtime,
            tool_registry=active_tool_registry,
            settings=_current_service_settings(),
            model_client=getattr(service, "model_client", None),
            record_event=_record_cron_delivery_event,
            cancellation_checker=cancellation_checker,
        )

    def _send_cron_platform_delivery(
        job,
        channel: str,
        target: str,
        text: str,
        *,
        run_label: str = "Scheduled task",
    ) -> bool:
        from nullion.cron_delivery import scheduled_task_delivery_text

        target = str(target or "").strip() or _cron_delivery_target(job, channel)
        current_settings = _current_service_settings()
        if not target:
            return False
        message = scheduled_task_delivery_text(job, text, run_label=run_label)
        if channel == "telegram":
            if not current_settings.telegram.bot_token:
                return False
            return _run_async_sync(lambda: _send_operator_telegram_delivery(
                current_settings.telegram.bot_token,
                target,
                message,
                principal_id=f"telegram:{target}",
                suppress_link_preview=True,
            ))
        if channel == "slack":
            if not current_settings.slack.bot_token:
                return False
            from nullion.slack_app import send_slack_platform_delivery

            return _run_async_sync(lambda: send_slack_platform_delivery(
                bot_token=current_settings.slack.bot_token,
                channel=target,
                text=message,
                principal_id=f"slack:{target}",
            ))
        if channel == "discord":
            if not current_settings.discord.bot_token:
                return False
            from nullion.discord_app import send_discord_platform_delivery

            return _run_async_sync(lambda: send_discord_platform_delivery(
                bot_token=current_settings.discord.bot_token,
                channel_id=target,
                text=message,
                principal_id=f"discord:{target}",
            ))
        return False

    def _send_manual_cron_status_update(
        job,
        group_id: str,
        status_text: str,
        *,
        terminal: bool,
        channel: str,
        target: str,
    ) -> bool:
        if channel != "telegram":
            return False
        target = str(target or "").strip()
        group_id = str(group_id or "").strip()
        status_text = str(status_text or "").strip()
        if not target or not group_id or not status_text:
            return False
        current_settings = _current_service_settings()
        bot_token = str(current_settings.telegram.bot_token or "").strip()
        if not bot_token:
            return False
        try:
            from nullion.platform_activity import PlatformTaskCardStore, platform_activity_capabilities, should_deliver_task_status
            from nullion.telegram_app import (
                _send_or_edit_telegram_task_status_message,
                _schedule_or_run_telegram_status_delivery,
                _telegram_allows_status_streaming,
                activity_trace_enabled_for_chat,
            )
        except Exception:
            logger.debug("Could not import Telegram status delivery helpers", exc_info=True)
            return False
        try:
            if not _telegram_allows_status_streaming(runtime, chat_id=target):
                return False
            include_activity = activity_trace_enabled_for_chat(runtime, chat_id=target)
            if not should_deliver_task_status(
                status_kind="task_summary",
                planner_feed_enabled=True,
                include_activity=include_activity,
            ):
                return False
            task_card_store = getattr(service, "_task_card_store", None)
            if task_card_store is None:
                task_card_store = PlatformTaskCardStore(platform_activity_capabilities("telegram"))
            rendered_status = task_card_store.update(
                target_id=target,
                group_id=group_id,
                status_kind="task_summary",
                text=status_text,
                include_activity=include_activity,
            )
            if not rendered_status:
                return False

            async def _deliver_status(_loop_bound_state: bool) -> bool:
                delivered = False
                try:
                    delivered = bool(await _send_or_edit_telegram_task_status_message(
                        bot_token,
                        getattr(service, "_status_messages", {}),
                        chat_id=target,
                        group_id=group_id,
                        text=rendered_status,
                        runtime=runtime,
                        bot_token=bot_token,
                        status_texts=getattr(service, "_status_texts", None),
                        status_locks=None,
                        typing_tasks=None,
                    ))
                    return delivered
                finally:
                    if terminal and delivered:
                        try:
                            task_card_store.clear(target_id=target, group_id=group_id)
                        except Exception:
                            logger.debug("Could not clear manual cron status card state", exc_info=True)

            return _schedule_or_run_telegram_status_delivery(_deliver_status, confirm=True)
        except Exception:
            logger.debug("Could not send manual cron status update for %s", getattr(job, "id", ""), exc_info=True)
            return False

    def _save_cron_web_delivery(job, conv_id: str, text: str, artifacts: object, result: dict) -> bool:
        _ = (job, conv_id, text, artifacts, result)
        return False

    def _manual_telegram_delivery_target(invocation) -> str:
        principal_id = str(getattr(invocation, "principal_id", "") or "").strip()
        if principal_id.startswith("telegram:"):
            target = principal_id.removeprefix("telegram:").strip()
            if target:
                return target
        if principal_id.startswith("user:"):
            try:
                from nullion.users import load_user_registry

                user_id = principal_id.removeprefix("user:")
                for user in load_user_registry(settings=_current_service_settings()).users:
                    if user.user_id != user_id or not user.active:
                        continue
                    channel = str(user.messaging_channel or "telegram").strip().lower()
                    target = str(user.telegram_chat_id or user.messaging_user_id or "").strip()
                    if channel == "telegram" and target:
                        return target
            except Exception:
                logger.debug("Could not resolve manual Telegram cron target from invocation", exc_info=True)
        return _operator_chat_id or ""

    def _cron_tool_runner(job, invocation=None):
        from nullion.cron_delivery import (
            CronRunDeliveryCallbacks,
            cron_delivery_block_reason,
            cron_conversation_id,
            manual_cron_silent_delivery_text,
            run_cron_delivery_workflow,
            start_manual_cron_background_delivery,
        )

        manual_target = _manual_telegram_delivery_target(invocation)

        def _manual_effective_channel(cron_job) -> str:
            principal_id = str(getattr(invocation, "principal_id", "") or "").strip()
            return "telegram" if manual_target and principal_id.startswith(("telegram:", "user:")) else _effective_cron_delivery_channel(cron_job)

        def _manual_delivery_target(cron_job, channel: str) -> str:
            if channel == "telegram" and manual_target:
                return manual_target
            return _cron_delivery_target(cron_job, channel)

        def _start_manual_cron_monitor() -> Callable[[], None] | None:
            monitor_seconds = _manual_cron_doctor_monitor_seconds()
            monitor_fired = threading.Event()
            monitor_finished = threading.Event()
            monitor_timer: threading.Timer | None = None

            def _notify_manual_cron_monitoring() -> None:
                if monitor_finished.is_set() or monitor_fired.is_set():
                    return
                monitor_fired.set()
                channel = _manual_effective_channel(job)
                target = _manual_delivery_target(job, channel)
                try:
                    _report_manual_cron_monitoring(
                        runtime,
                        job=job,
                        invocation=invocation,
                        channel=channel,
                        target=target,
                        elapsed_seconds=monitor_seconds,
                    )
                except Exception:
                    logger.debug("Could not report manual cron monitoring signal", exc_info=True)
                try:
                    _send_cron_platform_delivery(
                        job,
                        channel,
                        target,
                        _manual_cron_monitoring_text(job, elapsed_seconds=monitor_seconds),
                        run_label="Doctor monitoring",
                    )
                except Exception:
                    logger.debug("Could not deliver manual cron monitoring notice", exc_info=True)

            if monitor_seconds > 0:
                monitor_timer = threading.Timer(monitor_seconds, _notify_manual_cron_monitoring)
                monitor_timer.name = "nullion-manual-cron-doctor-monitor"
                monitor_timer.daemon = True
                monitor_timer.start()

            def _cleanup_monitor() -> None:
                monitor_finished.set()
                if monitor_timer is not None:
                    monitor_timer.cancel()

            return _cleanup_monitor

        channel = _manual_effective_channel(job)
        target = _manual_delivery_target(job, channel)
        origin_conversation_id = cron_conversation_id(job, channel, target)

        return start_manual_cron_background_delivery(
            job,
            label="Manual scheduled task run",
            callbacks=CronRunDeliveryCallbacks(
                effective_channel=_manual_effective_channel,
                delivery_target=_manual_delivery_target,
                run_agent_turn=lambda cron_job, conv_id, cancellation_checker=None: _run_cron_agent_turn(
                    cron_job,
                    conv_id,
                    label="Manual scheduled task run",
                    cancellation_checker=cancellation_checker,
                ),
                record_event=_record_cron_delivery_event,
                block_reason=cron_delivery_block_reason,
                save_web_delivery=_save_cron_web_delivery,
                send_platform_delivery=lambda cron_job, channel, target, text: _send_cron_platform_delivery(
                    cron_job,
                    channel,
                    target,
                    text,
                    run_label="Manual scheduled task run",
                ),
                start_background_delivery=None,
                clear_background_delivery=None,
                silent_delivery_text=manual_cron_silent_delivery_text,
                notify_approval_required=_notify_cron_approval_required,
                record_chat_turn=_record_cron_delivery_chat_turn,
            ),
            origin_conversation_id=origin_conversation_id,
            thread_name_prefix="nullion-telegram-manual-cron",
            before_run=_start_manual_cron_monitor,
            workflow_runner=run_cron_delivery_workflow,
            background_agent_conversation_id=origin_conversation_id,
            status_update_callback=lambda group_id, status_text, terminal: _send_manual_cron_status_update(
                job,
                group_id,
                status_text,
                terminal=terminal,
                channel=channel,
                target=target,
            ),
            status_update_interval_seconds=6.0,
            background_start_grace_seconds=0.0,
        )

    register_cron_tools(
        active_tool_registry,
        cron_runner=_cron_tool_runner,
        default_delivery_channel="telegram" if _operator_chat_id else "",
        default_delivery_target=_operator_chat_id or "",
    )
    try:
        from nullion.startup_warmup import schedule_chat_startup_warmup

        warm_principal_ids: list[str] = ["telegram_chat"]
        if _operator_chat_id:
            try:
                warm_principal_ids.append(principal_id_for_messaging_identity("telegram", _operator_chat_id, settings))
            except Exception:
                logger.debug("Could not resolve Telegram startup warmup principal", exc_info=True)

        schedule_chat_startup_warmup(
            runtime,
            registry=active_tool_registry,
            settings=settings,
            surface="telegram",
            principal_ids=tuple(dict.fromkeys(warm_principal_ids)),
        )
    except Exception:
        logger.debug("Could not schedule Telegram chat startup warmup", exc_info=True)
    return service


def build_runtime_service_from_settings(
    *,
    checkpoint_path: str | Path,
    env_path: str | Path | None = None,
):
    return _build_runtime_service_from_settings(
        checkpoint_path=checkpoint_path,
        env_path=env_path,
        service_factory=build_telegram_operator_service,
    )


def build_messaging_runtime_service_from_settings(
    *,
    checkpoint_path: str | Path,
    env_path: str | Path | None = None,
):
    return _build_runtime_service_from_settings(
        checkpoint_path=checkpoint_path,
        env_path=env_path,
        service_factory=build_messaging_operator_service,
    )


_MAX_BACKOFF_SECONDS: float = 60.0
_CONSECUTIVE_FAILURES_BEFORE_REBUILD: int = 3


async def _send_operator_telegram_message(bot_token: str, chat_id: str, text: str) -> None:
    """Fire-and-forget: send a single Telegram message to the operator chat."""
    try:
        from telegram import Bot  # type: ignore[import]
        from nullion.telegram_formatting import format_telegram_text

        formatted_text, message_kwargs = format_telegram_text(text)
        async with build_telegram_bot(Bot, bot_token) as bot:
            await retry_messaging_delivery_operation(
                lambda: bot.send_message(chat_id, formatted_text, **message_kwargs)
            )
    except Exception:
        logger.warning("Failed to deliver operator notification message", exc_info=True)


async def _send_operator_telegram_delivery(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    principal_id: str | None = None,
    reply_markup=None,
    suppress_link_preview: bool = False,
    parse_mode: str | None = None,
    request_id: str | None = None,
) -> bool:
    """Send a Telegram delivery, uploading any MEDIA artifact directives."""
    delivery = None
    try:
        from telegram import Bot  # type: ignore[import]

        from nullion.messaging_adapters import (
            build_platform_delivery_receipt,
            delivery_receipt_transport_succeeded,
            prepare_reply_for_platform_delivery,
            record_platform_delivery_receipt,
            telegram_attachment_upload_limit_bytes,
        )
        from nullion.telegram_formatting import format_telegram_text

        delivery = prepare_reply_for_platform_delivery(
            text,
            principal_id=principal_id,
            max_attachment_bytes=telegram_attachment_upload_limit_bytes(),
        )
        async with build_telegram_bot(Bot, bot_token) as bot:
            if delivery.attachments:
                caption = delivery.text
                sent_message_id = None
                for index, attachment_path in enumerate(delivery.attachments):
                    caption_text = caption[:1024] if caption and index == 0 else None
                    caption_kwargs = {}
                    if caption_text:
                        caption_text, caption_kwargs = format_telegram_text(caption_text)
                    if index == 0 and reply_markup is not None:
                        caption_kwargs = {**caption_kwargs, "reply_markup": reply_markup}
                    async def send_document(
                        attachment_path=attachment_path,
                        caption_text=caption_text,
                        caption_kwargs=caption_kwargs,
                    ):
                        with attachment_path.open("rb") as document:
                            return await bot.send_document(
                                chat_id,
                                document,
                                caption=caption_text,
                                **caption_kwargs,
                            )

                    sent_message = await retry_messaging_delivery_operation(send_document)
                    if sent_message_id is None:
                        sent_message_id = getattr(sent_message, "message_id", None)
                receipt = build_platform_delivery_receipt(
                    channel="telegram",
                    target_id=str(chat_id),
                    delivery=delivery,
                    transport_ok=True,
                    request_id=request_id,
                    message_id=None if sent_message_id is None else str(sent_message_id),
                )
                record_platform_delivery_receipt(receipt)
                return delivery_receipt_transport_succeeded(receipt)
            message_kwargs = {}
            if reply_markup is not None:
                message_kwargs["reply_markup"] = reply_markup
            if suppress_link_preview:
                message_kwargs["disable_web_page_preview"] = True
            if parse_mode:
                message_kwargs["parse_mode"] = parse_mode
                message_text = text
            else:
                message_text, formatting_kwargs = format_telegram_text(delivery.text or "")
                message_kwargs.update(formatting_kwargs)
            sent_message = await retry_messaging_delivery_operation(
                lambda: bot.send_message(chat_id, message_text, **message_kwargs)
            )
            sent_message_id = getattr(sent_message, "message_id", None)
            receipt = build_platform_delivery_receipt(
                channel="telegram",
                target_id=str(chat_id),
                delivery=delivery,
                transport_ok=True,
                request_id=request_id,
                message_id=None if sent_message_id is None else str(sent_message_id),
            )
            record_platform_delivery_receipt(receipt)
            return delivery_receipt_transport_succeeded(receipt)
    except Exception:
        logger.warning("Failed to deliver operator notification message", exc_info=True)
        if delivery is not None:
            try:
                from nullion.messaging_adapters import build_platform_delivery_receipt, record_platform_delivery_receipt

                record_platform_delivery_receipt(
                    build_platform_delivery_receipt(
                        channel="telegram",
                        target_id=str(chat_id),
                        delivery=delivery,
                        transport_ok=False,
                        request_id=request_id,
                        error="platform_delivery_failed",
                    )
                )
            except Exception:
                pass
        return False


def _try_notify_recovery(service) -> None:
    """Send a 'back online' notification to the operator chat after a service rebuild."""
    try:
        chat_id = getattr(service, "operator_chat_id", None)
        bot_token = getattr(service, "bot_token", None)
        if not chat_id or not bot_token:
            return
        asyncio.run(
            _send_operator_telegram_message(bot_token, chat_id, "🟢 Nullion is back online.")
        )
    except RuntimeError:
        # asyncio.run() raises RuntimeError if there's already a running loop — skip silently
        logger.debug("Skipped recovery notification (event loop conflict)", exc_info=True)
    except Exception:
        logger.warning("Could not send recovery notification", exc_info=True)


def _complete_gateway_restart_marker(service) -> None:
    try:
        from nullion.gateway_notifications import complete_gateway_restart_if_needed

        settings = getattr(service, "settings", None)
        if settings is None:
            return
        complete_gateway_restart_if_needed(settings=settings)
    except Exception:
        logger.debug("Could not complete gateway restart lifecycle marker", exc_info=True)


def main(
    *,
    checkpoint_path: str | Path = _DEFAULT_CHECKPOINT_PATH,
    env_path: str | Path | None = _DEFAULT_ENV_PATH,
    service_builder=build_runtime_service_from_settings,
    polling_retry_delay_seconds: float = 1.0,
    max_backoff_seconds: float = _MAX_BACKOFF_SECONDS,
    max_consecutive_failures_before_rebuild: int = _CONSECUTIVE_FAILURES_BEFORE_REBUILD,
):
    checkpoint = Path(checkpoint_path)
    env_path_value = None if env_path is None else Path(env_path)
    startup_settings = load_settings(env_path=env_path)
    if not startup_settings.telegram.chat_enabled:
        logger.info(
            "Nullion Telegram operator disabled by NULLION_TELEGRAM_CHAT_ENABLED=false "
            "(checkpoint=%s, env_path=%s)",
            checkpoint,
            env_path_value,
        )
        return

    # ── SIGHUP → graceful service rebuild on next loop iteration ─────────────
    _sighup_requested = False

    def _handle_sighup(signum, frame):  # noqa: ARG001
        nonlocal _sighup_requested
        _sighup_requested = True
        logger.info("SIGHUP received — service rebuild queued")

    try:
        signal.signal(signal.SIGHUP, _handle_sighup)
    except (OSError, ValueError):
        pass  # Windows or non-main-thread — SIGHUP not available

    # ── Initial build (fail-fast; launchd / systemd will restart us) ─────────
    try:
        service = service_builder(checkpoint_path=checkpoint, env_path=env_path)
    except Exception:
        logger.exception(
            "Failed to start Nullion Telegram operator (checkpoint=%s, env_path=%s)",
            checkpoint,
            env_path_value,
        )
        raise

    logger.info(
        "Starting Nullion Telegram operator (checkpoint=%s, env_path=%s, operator_chat_id=%s)",
        checkpoint,
        env_path_value,
        service.operator_chat_id,
    )
    _complete_gateway_restart_marker(service)

    consecutive_failures = 0
    backoff = max(polling_retry_delay_seconds, 1.0)

    while True:
        # ── SIGHUP-triggered rebuild ──────────────────────────────────────────
        if _sighup_requested:
            _sighup_requested = False
            logger.info("Rebuilding service due to SIGHUP (checkpoint=%s)", checkpoint)
            try:
                service = service_builder(checkpoint_path=checkpoint, env_path=env_path)
                consecutive_failures = 0
                backoff = max(polling_retry_delay_seconds, 1.0)
                logger.info("Service rebuilt successfully after SIGHUP")
            except Exception:
                logger.exception(
                    "Service rebuild after SIGHUP failed; continuing with existing service (checkpoint=%s)",
                    checkpoint,
                )

        try:
            service.run_polling()
            # run_polling() returned normally — unexpected but non-fatal
            consecutive_failures = 0
            backoff = max(polling_retry_delay_seconds, 1.0)
            logger.warning(
                "Telegram polling returned unexpectedly; restarting "
                "(checkpoint=%s, env_path=%s, operator_chat_id=%s)",
                checkpoint,
                env_path_value,
                service.operator_chat_id,
            )
            continue

        except KeyboardInterrupt:
            logger.warning(
                "Nullion Telegram operator interrupted "
                "(checkpoint=%s, env_path=%s, operator_chat_id=%s)",
                checkpoint,
                env_path_value,
                service.operator_chat_id,
            )
            raise

        except Exception as exc:
            consecutive_failures += 1
            is_transient = _is_transient_telegram_polling_timeout(exc)
            sleep_time = min(backoff, max_backoff_seconds)

            if is_transient:
                logger.warning(
                    "Transient Telegram polling timeout; retrying in %.1fs "
                    "(failure=%d, checkpoint=%s, env_path=%s, operator_chat_id=%s)",
                    sleep_time,
                    consecutive_failures,
                    checkpoint,
                    env_path_value,
                    service.operator_chat_id,
                    exc_info=True,
                )
            else:
                logger.error(
                    "Nullion Telegram polling failed; retrying in %.1fs "
                    "(failure=%d, checkpoint=%s, env_path=%s, operator_chat_id=%s)",
                    sleep_time,
                    consecutive_failures,
                    checkpoint,
                    env_path_value,
                    service.operator_chat_id,
                    exc_info=True,
                )

            # ── Rebuild service after too many consecutive failures ───────────
            if consecutive_failures >= max_consecutive_failures_before_rebuild:
                logger.warning(
                    "Attempting service rebuild after %d consecutive failures (checkpoint=%s)",
                    consecutive_failures,
                    checkpoint,
                )
                prev_failures = consecutive_failures
                try:
                    service = service_builder(checkpoint_path=checkpoint, env_path=env_path)
                    consecutive_failures = 0
                    backoff = max(polling_retry_delay_seconds, 1.0)
                    logger.info(
                        "Service rebuilt successfully after %d consecutive failures (checkpoint=%s)",
                        prev_failures,
                        checkpoint,
                    )
                    _try_notify_recovery(service)
                except Exception:
                    logger.exception(
                        "Service rebuild failed; continuing with exponential backoff "
                        "(checkpoint=%s, failure=%d)",
                        checkpoint,
                        consecutive_failures,
                    )

            # ── Exponential backoff ───────────────────────────────────────────
            if sleep_time > 0:
                time.sleep(sleep_time)
            backoff = min(backoff * 2, max_backoff_seconds)


def cli(argv: list[str] | None = None, *, runner=main):
    def _run() -> None:
        parser = argparse.ArgumentParser(prog="nullion-telegram")
        parser.add_argument("--checkpoint", default=str(_DEFAULT_CHECKPOINT_PATH))
        parser.add_argument("--env-file", default=str(_DEFAULT_ENV_PATH))
        args = parser.parse_args(argv)

        def _run_telegram() -> None:
            runner(
                checkpoint_path=Path(args.checkpoint),
                env_path=Path(args.env_file),
                service_builder=build_runtime_service_from_settings,
            )
            return None

        return run_single_instance_entrypoint(
            "telegram",
            _run_telegram,
            wait_seconds=1.0,
            description="nullion-telegram",
        )

    return run_user_facing_entrypoint(_run)


__all__ = ["build_runtime_service_from_settings", "cli", "main"]


if __name__ == "__main__":
    cli()

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

from nullion.config import ProviderBinding, load_env_file_into_environ, load_settings
from nullion.entrypoint_guard import run_single_instance_entrypoint, run_user_facing_entrypoint
from nullion.artifacts import ensure_artifact_root
from nullion.messaging_adapters import messaging_file_allowed_roots
from nullion.providers import resolve_plugin_provider_kwargs
from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_persistence import list_runtime_store_backups, restore_runtime_store_backup
from nullion.telegram_app import build_messaging_operator_service, build_telegram_operator_service
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


logger = logging.getLogger(__name__)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_ENV_PATH = Path.home() / ".nullion" / ".env"
_DEFAULT_CHECKPOINT_PATH = Path.home() / ".nullion" / "runtime.db"


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
        return bootstrap_persistent_runtime(checkpoint)
    logger.warning(
        "No restorable runtime checkpoint backup found; starting with a new runtime "
        "(checkpoint=%s, corrupt_backup=%s)",
        checkpoint,
        candidate,
    )
    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.checkpoint()
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
    if env_path is not None:
        load_env_file_into_environ(env_path, override=True)
    settings = load_settings(env_path=env_path)
    try:
        runtime = bootstrap_persistent_runtime(checkpoint)
    except (ValueError, sqlite3.Error) as exc:
        runtime = _recover_runtime_from_corrupt_checkpoint(checkpoint, exc)
    workspace_root = (
        Path(settings.workspace_root).expanduser()
        if isinstance(settings.workspace_root, str) and settings.workspace_root.strip()
        else checkpoint.parent
    )
    artifact_root = ensure_artifact_root(runtime)
    registry_factory_kwargs = {
        "terminal_executor_backend": _terminal_executor_backend_from_settings(settings),
        "workspace_root": workspace_root,
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
    browser_backend = _resolve_browser_backend()
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

    def _effective_cron_delivery_channel(job) -> str:
        from nullion.cron_delivery import effective_cron_delivery_channel
        from nullion.config import load_settings as _load_settings

        try:
            current_settings = _load_settings()
        except Exception:
            current_settings = settings
        return effective_cron_delivery_channel(job, settings=current_settings)

    def _cron_delivery_target(job, channel: str) -> str:
        from nullion.cron_delivery import cron_delivery_target
        from nullion.config import load_settings as _load_settings

        try:
            current_settings = _load_settings()
        except Exception:
            current_settings = settings
        return cron_delivery_target(job, channel, settings=current_settings)

    def _cron_result_block_reason(result: dict, text: str, artifacts: object) -> str | None:
        if result.get("reached_iteration_limit"):
            return "cron_run_reached_iteration_limit"
        if result.get("suspended_for_approval"):
            return "waiting_for_approval"
        _ = (text, artifacts)
        return None

    def _cron_agent_history(conv_id: str) -> list[dict]:
        from nullion.artifacts import artifact_root_for_principal
        from nullion.connections import format_workspace_connections_for_prompt
        from nullion.runtime_config import format_runtime_config_for_prompt
        from nullion.skill_pack_catalog import skill_pack_access_prompt
        from nullion.skill_pack_installer import format_enabled_skill_packs_for_prompt
        from nullion.system_context import build_system_context_snapshot, format_system_context_for_prompt
        from nullion.workspace_storage import format_workspace_storage_for_prompt

        history: list[dict] = []
        caps_text = format_system_context_for_prompt(build_system_context_snapshot(tool_registry=active_tool_registry))
        if caps_text:
            history.append({
                "role": "system",
                "content": [{"type": "text", "text": (
                    "You are Nullion, a security-first AI agent. Use only registered tools. "
                    "Below is the live inventory of tools registered in this session.\n\n"
                    + caps_text
                )}],
            })
        config_text = format_runtime_config_for_prompt(model_client=getattr(service, "model_client", None))
        if config_text:
            history.append({"role": "system", "content": [{"type": "text", "text": config_text}]})
        connections_text = format_workspace_connections_for_prompt(principal_id=conv_id)
        if connections_text:
            history.append({"role": "system", "content": [{"type": "text", "text": connections_text}]})
        skill_text = format_enabled_skill_packs_for_prompt(settings.enabled_skill_packs)
        access_text = skill_pack_access_prompt(settings.enabled_skill_packs, principal_id=conv_id)
        if access_text:
            skill_text = (skill_text + "\n\n" + access_text).strip()
        if skill_text:
            history.append({"role": "system", "content": [{"type": "text", "text": skill_text}]})
        artifact_root = artifact_root_for_principal(conv_id)
        storage_text = format_workspace_storage_for_prompt(principal_id=conv_id)
        history.append({
            "role": "system",
            "content": [{"type": "text", "text": (
                "Cron delivery contract: create requested deliverable files under this artifact directory "
                f"and attach them with explicit MEDIA lines: {artifact_root}. "
                "Keep scratch, checkpoint, and state files in the workspace unless they are requested deliverables.\n\n"
                f"{storage_text}"
            )}],
        })
        return history

    def _run_cron_agent_turn(job, conv_id: str, *, label: str) -> dict:
        from nullion.cron_delivery import cron_agent_prompt

        orchestrator = getattr(service, "agent_orchestrator", None)
        if orchestrator is None:
            return {"cron_run_failed": True, "reason": "agent_orchestrator_unavailable", "text": ""}
        result = orchestrator.run_turn(
            conversation_id=conv_id,
            principal_id=conv_id,
            user_message=cron_agent_prompt(job, label=label),
            conversation_history=_cron_agent_history(conv_id),
            tool_registry=active_tool_registry,
            policy_store=runtime.store,
            approval_store=runtime.store,
        )
        payload = {
            "text": result.final_text or "",
            "tool_results": list(result.tool_results),
            "artifacts": list(result.artifacts),
            "suspended_for_approval": result.suspended_for_approval,
            "approval_id": result.approval_id,
            "reached_iteration_limit": result.reached_iteration_limit,
        }
        return payload

    def _send_cron_platform_delivery(job, channel: str, text: str, *, run_label: str = "Scheduled task") -> bool:
        from nullion.cron_delivery import scheduled_task_delivery_text

        if channel != "telegram":
            return False
        target = _cron_delivery_target(job, channel)
        if not target or not settings.telegram.bot_token:
            return False
        message = scheduled_task_delivery_text(job, text, run_label=run_label)
        return _run_async_sync(lambda: _send_operator_telegram_delivery(
            settings.telegram.bot_token,
            target,
            message,
            principal_id=f"telegram:{target}",
            suppress_link_preview=True,
        ))

    def _save_cron_web_delivery(job, conv_id: str, text: str, artifacts: object, result: dict) -> bool:
        _ = (job, conv_id, text, artifacts, result)
        return False

    def _cron_tool_runner(job):
        from nullion.cron_delivery import CronRunDeliveryCallbacks, run_cron_delivery_workflow

        return run_cron_delivery_workflow(
            job,
            label="Manual scheduled task run",
            callbacks=CronRunDeliveryCallbacks(
                effective_channel=_effective_cron_delivery_channel,
                delivery_target=_cron_delivery_target,
                run_agent_turn=lambda cron_job, conv_id: _run_cron_agent_turn(cron_job, conv_id, label="Manual scheduled task run"),
                record_event=_record_cron_delivery_event,
                block_reason=_cron_result_block_reason,
                save_web_delivery=_save_cron_web_delivery,
                send_platform_delivery=lambda cron_job, channel, text: _send_cron_platform_delivery(
                    cron_job,
                    channel,
                    text,
                    run_label="Manual scheduled task run",
                ),
                start_background_delivery=None,
                clear_background_delivery=None,
            ),
        )

    register_cron_tools(
        active_tool_registry,
        cron_runner=_cron_tool_runner,
        default_delivery_channel="telegram" if _operator_chat_id else "",
        default_delivery_target=_operator_chat_id or "",
    )
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
        async with Bot(bot_token) as bot:
            await bot.send_message(chat_id, formatted_text, **message_kwargs)
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
) -> bool:
    """Send a Telegram delivery, uploading any MEDIA artifact directives."""
    delivery = None
    try:
        from telegram import Bot  # type: ignore[import]

        from nullion.messaging_adapters import (
            build_platform_delivery_receipt,
            prepare_reply_for_platform_delivery,
            record_platform_delivery_receipt,
        )
        from nullion.telegram_formatting import format_telegram_text

        delivery = prepare_reply_for_platform_delivery(text, principal_id=principal_id)
        async with Bot(bot_token) as bot:
            if delivery.attachments:
                caption = delivery.text
                for index, attachment_path in enumerate(delivery.attachments):
                    caption_text = caption[:1024] if caption and index == 0 else None
                    caption_kwargs = {}
                    if caption_text:
                        caption_text, caption_kwargs = format_telegram_text(caption_text)
                    with attachment_path.open("rb") as document:
                        await bot.send_document(
                            chat_id,
                            document,
                            caption=caption_text,
                            **caption_kwargs,
                        )
                record_platform_delivery_receipt(
                    build_platform_delivery_receipt(
                        channel="telegram",
                        target_id=str(chat_id),
                        delivery=delivery,
                        transport_ok=True,
                    )
                )
                return True
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
            await bot.send_message(chat_id, message_text, **message_kwargs)
            record_platform_delivery_receipt(
                build_platform_delivery_receipt(
                    channel="telegram",
                    target_id=str(chat_id),
                    delivery=delivery,
                    transport_ok=True,
                )
            )
            return True
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

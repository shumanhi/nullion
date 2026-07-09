"""Compact prompt context for connector-backed skill access."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import logging
import re
import threading
import time


logger = logging.getLogger(__name__)

_ACTIVE_CONNECTOR_APP_ID_CACHE_NAMESPACE = "connector.active_app_ids"
_ACTIVE_CONNECTOR_APP_ID_CACHE_VERSION = "v2"
_ACTIVE_CONNECTOR_APP_ID_CACHE_TTL_SECONDS = 10 * 60
_ACTIVE_CONNECTOR_APP_ID_CACHE_KEY = {"kind": "active_connector_app_ids"}
_ACTIVE_CONNECTOR_EMPTY_MEMORY_CACHE_TTL_SECONDS = 5
_ACTIVE_CONNECTOR_MEMORY_CACHE_LOCK = threading.RLock()
_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE: tuple[dict[str, tuple[str, ...]], ...] | None = None
_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT = 0.0
_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE: tuple[dict[str, object], ...] | None = None
_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT = 0.0


def _memory_cache_ttl_seconds(value: object) -> int:
    return (
        _ACTIVE_CONNECTOR_APP_ID_CACHE_TTL_SECONDS
        if value
        else _ACTIVE_CONNECTOR_EMPTY_MEMORY_CACHE_TTL_SECONDS
    )


def _active_connector_provider_summaries(
    providers: Iterable[Mapping[str, object]],
) -> tuple[dict[str, tuple[str, ...]], ...]:
    summaries: list[dict[str, tuple[str, ...]]] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        app_ids = tuple(
            dict.fromkeys(
                str(app_id or "").strip().lower()
                for app_id in (provider.get("active_app_ids") or ())
                if str(app_id or "").strip()
            )
        )
        structured_tools = tuple(
            dict.fromkeys(
                str(tool or "").strip()
                for tool in (provider.get("structured_tools") or ())
                if str(tool or "").strip()
            )
        )
        if not app_ids and not structured_tools:
            continue
        summary: dict[str, tuple[str, ...]] = {}
        if app_ids:
            summary["active_app_ids"] = app_ids
        if structured_tools:
            summary["structured_tools"] = structured_tools[:24]
        summaries.append(summary)
    return tuple(summaries[:8])


def _set_active_connector_app_id_memory_cache(providers: Iterable[Mapping[str, object]]) -> None:
    global _ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE
    global _ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT
    value = _active_connector_provider_summaries(providers)
    with _ACTIVE_CONNECTOR_MEMORY_CACHE_LOCK:
        _ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE = value
        _ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT = time.monotonic() + _memory_cache_ttl_seconds(value)


def _get_active_connector_app_id_memory_cache() -> tuple[dict[str, tuple[str, ...]], ...] | None:
    with _ACTIVE_CONNECTOR_MEMORY_CACHE_LOCK:
        if _ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE is None:
            return None
        if time.monotonic() > _ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT:
            return None
        return _ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE


def _set_active_connector_provider_context_memory_cache(value: tuple[dict[str, object], ...]) -> None:
    global _ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE
    global _ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT
    with _ACTIVE_CONNECTOR_MEMORY_CACHE_LOCK:
        _ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE = value
        _ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT = (
            time.monotonic() + _memory_cache_ttl_seconds(value)
        )


def _get_active_connector_provider_context_memory_cache() -> tuple[dict[str, object], ...] | None:
    with _ACTIVE_CONNECTOR_MEMORY_CACHE_LOCK:
        if _ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE is None:
            return None
        if time.monotonic() > _ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT:
            return None
        return _ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE


def _active_app_ids_from_providers(providers: Iterable[Mapping[str, object]]) -> tuple[str, ...]:
    app_ids: list[str] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        for raw_app_id in provider.get("active_app_ids") or ():
            app_id = str(raw_app_id or "").strip().lower()
            if app_id and app_id not in app_ids:
                app_ids.append(app_id)
    return tuple(app_ids)


def cache_active_connector_app_ids(providers: Iterable[Mapping[str, object]]) -> None:
    provider_entries = tuple(providers)
    provider_summaries = _active_connector_provider_summaries(provider_entries)
    app_ids = _active_app_ids_from_providers(provider_entries)
    _set_active_connector_app_id_memory_cache(provider_summaries)
    try:
        from nullion import runtime_cache

        runtime_cache.set_json(
            _ACTIVE_CONNECTOR_APP_ID_CACHE_NAMESPACE,
            _ACTIVE_CONNECTOR_APP_ID_CACHE_KEY,
            [dict(summary) for summary in provider_summaries] if provider_summaries else list(app_ids),
            version=_ACTIVE_CONNECTOR_APP_ID_CACHE_VERSION,
            ttl_seconds=_ACTIVE_CONNECTOR_APP_ID_CACHE_TTL_SECONDS,
            persistent=True,
            max_entries=4,
        )
    except Exception:
        logger.debug("Could not cache active connector app ids", exc_info=True)


def cached_active_connector_app_id_providers(
    *,
    allow_persistent: bool = True,
) -> tuple[dict[str, tuple[str, ...]], ...]:
    memory_cached = _get_active_connector_app_id_memory_cache()
    if memory_cached is not None:
        return memory_cached
    if not allow_persistent:
        return ()
    try:
        from nullion import runtime_cache

        cached = runtime_cache.get_json(
            _ACTIVE_CONNECTOR_APP_ID_CACHE_NAMESPACE,
            _ACTIVE_CONNECTOR_APP_ID_CACHE_KEY,
            version=_ACTIVE_CONNECTOR_APP_ID_CACHE_VERSION,
            ttl_seconds=_ACTIVE_CONNECTOR_APP_ID_CACHE_TTL_SECONDS,
            persistent=True,
        )
    except Exception:
        return ()
    if not cached.hit or not isinstance(cached.value, list):
            _set_active_connector_app_id_memory_cache(())
            return ()
    if cached.value and all(isinstance(item, Mapping) for item in cached.value):
        provider_summaries = _active_connector_provider_summaries(
            item for item in cached.value if isinstance(item, Mapping)
        )
        _set_active_connector_app_id_memory_cache(provider_summaries)
        return provider_summaries
    app_ids = tuple(
        dict.fromkeys(str(app_id or "").strip().lower() for app_id in cached.value if str(app_id or "").strip())
    )
    provider_summaries = ({"active_app_ids": app_ids},) if app_ids else ()
    _set_active_connector_app_id_memory_cache(provider_summaries)
    if not provider_summaries:
        return ()
    return provider_summaries


def active_connector_provider_context_snapshot(
    *,
    allow_runtime_load: bool = True,
) -> tuple[dict[str, object], ...]:
    memory_cached = _get_active_connector_provider_context_memory_cache()
    if memory_cached is not None:
        return memory_cached
    if not allow_runtime_load:
        return ()
    try:
        from nullion.turn_context_policy import _active_connector_provider_context, _runtime_has_active_connector_provider
    except Exception:
        return ()
    try:
        if not _runtime_has_active_connector_provider():
            _set_active_connector_provider_context_memory_cache(())
            _set_active_connector_app_id_memory_cache(())
            return ()
        providers = _active_connector_provider_context()
    except Exception:
        logger.debug("Could not load active connector provider context", exc_info=True)
        return ()

    snapshots: list[dict[str, object]] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        provider_id = str(provider.get("provider_id") or "").strip()
        if not provider_id:
            continue
        structured_tools = tuple(
            dict.fromkeys(
                str(tool or "").strip()
                for tool in (provider.get("structured_tools") or ())
                if str(tool or "").strip()
            )
        )[:12]
        active_app_ids = tuple(
            dict.fromkeys(
                str(app_id or "").strip().lower()
                for app_id in (provider.get("active_app_ids") or ())
                if str(app_id or "").strip()
            )
        )[:80]
        reference_paths = tuple(
            str(path or "").strip()
            for path in (provider.get("reference_paths") or ())
            if str(path or "").strip()
        )
        try:
            reference_path_count = int(provider.get("reference_path_count") or len(reference_paths))
        except (TypeError, ValueError):
            reference_path_count = len(reference_paths)
        snapshot: dict[str, object] = {
            "provider_id": provider_id,
            "display_name": str(provider.get("display_name") or provider_id).strip(),
            "permission_mode": str(provider.get("permission_mode") or "").strip(),
            "structured_tools": structured_tools,
        }
        skill_pack_id = str(provider.get("skill_pack_id") or "").strip()
        if skill_pack_id:
            snapshot["skill_pack_id"] = skill_pack_id
        if active_app_ids:
            snapshot["active_app_ids"] = active_app_ids
        if reference_path_count:
            snapshot["reference_path_count"] = reference_path_count
        if reference_paths:
            snapshot["reference_paths_sample"] = reference_paths[:12]
        snapshots.append(snapshot)
    result = tuple(snapshots[:8])
    cache_active_connector_app_ids(result)
    _set_active_connector_provider_context_memory_cache(result)
    return result


def format_active_connector_provider_context_for_prompt(
    providers: Iterable[Mapping[str, object]],
) -> str | None:
    provider_entries = tuple(providers)
    if not provider_entries:
        return None

    lines = [
        "Associated connector skill context:",
        "- These entries are runtime facts from active connector providers and installed skill-pack references.",
        "- If the user asks whether an external app/source/account can be accessed and that app/source appears in active_app_ids or attached reference paths, do not answer that it is unavailable merely because no first-class native tool is visible.",
        "- First request connector scope with source_user_requested=true and connector_app_ids for the matching normalized app ids when request_tool_scope is visible; then use the exposed structured account tools, skill_pack_read, and connector_request as appropriate.",
        "- If the exact access path is still unavailable or authorization is missing, say there is no direct native connector/access yet, then offer the Builder path plainly: Builder can create or save a reusable skill or connector workflow for that source once the user approves or connects the needed account. Do not substitute terminal_exec or an unrelated generic connector for external account access.",
    ]
    for provider in provider_entries:
        details = [
            f"provider_id={provider.get('provider_id')}",
            f"display_name={provider.get('display_name')}",
        ]
        if provider.get("permission_mode"):
            details.append(f"permission_mode={provider.get('permission_mode')}")
        if provider.get("skill_pack_id"):
            details.append(f"skill_pack_id={provider.get('skill_pack_id')}")
        structured_tools = ", ".join(str(tool) for tool in (provider.get("structured_tools") or ()))
        if structured_tools:
            details.append(f"structured_tools=[{structured_tools}]")
        active_app_ids = ", ".join(str(app_id) for app_id in (provider.get("active_app_ids") or ()))
        if active_app_ids:
            details.append(f"active_app_ids=[{active_app_ids}]")
        reference_path_count = provider.get("reference_path_count")
        if reference_path_count:
            details.append(f"reference_path_count={reference_path_count}")
        reference_sample = ", ".join(str(path) for path in (provider.get("reference_paths_sample") or ()))
        if reference_sample:
            details.append(f"reference_paths_sample=[{reference_sample}]")
        lines.append(f"- {'; '.join(details)}")
    return "\n".join(lines)


def mentioned_connector_app_ids(text: object, providers: Iterable[Mapping[str, object]]) -> tuple[str, ...]:
    raw_text = str(text or "").lower()
    text_tokens = re.findall(r"[a-z0-9]+", raw_text)
    if not text_tokens:
        return ()
    text_token_set = set(text_tokens)
    compact_text = "".join(text_tokens)
    generic_app_id_tokens = {
        "app",
        "api",
        "cloud",
        "co",
        "com",
        "connector",
        "dev",
        "io",
        "net",
        "org",
        "service",
        "services",
    }
    mentioned: list[str] = []
    for provider in providers:
        raw_app_ids = provider.get("active_app_ids") if isinstance(provider, Mapping) else None
        for raw_app_id in raw_app_ids if isinstance(raw_app_ids, (list, tuple)) else ():
            app_id = str(raw_app_id or "").strip().lower()
            if not app_id or app_id in mentioned:
                continue
            app_tokens = re.findall(r"[a-z0-9]+", app_id)
            if not app_tokens:
                continue
            if len(app_tokens) == 1:
                if app_tokens[0] in text_token_set:
                    mentioned.append(app_id)
                continue
            if "".join(app_tokens) in compact_text:
                mentioned.append(app_id)
                continue
            # App ids often include a provider prefix plus a user-visible source
            # token, for example "vendor-calendar". Treat those app-id-derived
            # source tokens as structured metadata, not product routing prose.
            if any(
                token in text_token_set
                for token in app_tokens[1:]
                if len(token) >= 3 and token not in generic_app_id_tokens
            ):
                mentioned.append(app_id)
    return tuple(mentioned)


__all__ = [
    "active_connector_provider_context_snapshot",
    "cache_active_connector_app_ids",
    "cached_active_connector_app_id_providers",
    "format_active_connector_provider_context_for_prompt",
    "mentioned_connector_app_ids",
]

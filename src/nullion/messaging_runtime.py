"""Shared runtime builder for messaging adapters."""

from __future__ import annotations

from pathlib import Path


def build_messaging_runtime_service_from_settings(
    *,
    checkpoint_path: str | Path,
    env_path: str | Path | None = None,
):
    from nullion.telegram_entrypoint import build_messaging_runtime_service_from_settings as _build_service

    return _build_service(checkpoint_path=checkpoint_path, env_path=env_path)


__all__ = ["build_messaging_runtime_service_from_settings"]

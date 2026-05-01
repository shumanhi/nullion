from __future__ import annotations

import pathlib
import sys

import pytest


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


@pytest.fixture(autouse=True)
def _isolate_nullion_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "nullion-data"))


@pytest.fixture(autouse=True)
def _block_live_restart_side_effects(monkeypatch):
    from nullion import desktop_entrypoint, service_control

    monkeypatch.setattr(desktop_entrypoint, "schedule_desktop_reload", lambda **kwargs: None)

    def blocked_service_command(args, *, timeout=15.0):  # noqa: ANN001, ARG001
        raise AssertionError(f"test attempted to run a live managed-service command: {args!r}")

    monkeypatch.setattr(service_control, "_run", blocked_service_command)

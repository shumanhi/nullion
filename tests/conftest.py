from __future__ import annotations

import pathlib
import sys
import os
import threading

import pytest


APP_ROOT = pathlib.Path(
    os.environ.get(
        "NULLION_APP_REPO",
        pathlib.Path(__file__).resolve().parents[1],
    )
).resolve()
os.chdir(APP_ROOT)
sys.path.insert(0, str(APP_ROOT / "src"))
sys.path.insert(0, str(APP_ROOT))


@pytest.fixture(autouse=True)
def _isolate_nullion_data_dir(monkeypatch, tmp_path):
    # Settings routes deliberately update os.environ. Those writes bypass
    # monkeypatch and must not disable capabilities in subsequent tests.
    original_environment = dict(os.environ)
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "nullion-data"))
    yield
    monkeypatch.undo()
    os.environ.clear()
    os.environ.update(original_environment)


@pytest.fixture(autouse=True)
def _close_test_timers(monkeypatch, _isolate_nullion_data_dir):
    # Timers resolve patched functions when they fire. They must finish or
    # be cancelled while their originating test's patches are still active.
    timers = []
    timer_type = threading.Timer
    original_start = threading.Thread.start

    def start(thread, *args, **kwargs):
        result = original_start(thread, *args, **kwargs)
        if isinstance(thread, timer_type):
            timers.append(thread)
        return result

    monkeypatch.setattr(threading.Thread, "start", start)
    yield
    for timer in timers:
        timer.cancel()
        timer.join(timeout=3)
        assert not timer.is_alive(), "Test timer did not stop before restoring patches"


@pytest.fixture(autouse=True)
def _block_live_restart_side_effects(monkeypatch):
    from nullion import desktop_entrypoint, service_control

    monkeypatch.setattr(desktop_entrypoint, "schedule_desktop_reload", lambda **kwargs: None)

    def blocked_service_command(args, *, timeout=15.0):  # noqa: ANN001, ARG001
        raise AssertionError(f"test attempted to run a live managed-service command: {args!r}")

    monkeypatch.setattr(service_control, "_run", blocked_service_command)

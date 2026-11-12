from __future__ import annotations

import pathlib
import sys
import os
import threading

import pytest


APP_ROOT = pathlib.Path(
    os.environ.get(
        "NULLION_APP_REPO",
        (pathlib.Path(__file__).resolve().parents[1] if (pathlib.Path(__file__).resolve().parents[1] / "src/nullion").is_dir()
         else pathlib.Path(__file__).resolve().parents[3] / "nullion"),
    )
).resolve()
os.chdir(APP_ROOT)
sys.path.insert(0, str(APP_ROOT / "src"))
sys.path.insert(0, str(APP_ROOT))

# Refuse mixed-checkout validation instead of silently importing another lane.
import nullion
if pathlib.Path(nullion.__file__).resolve().parent != (APP_ROOT / 'src/nullion').resolve():
    raise RuntimeError('Test runtime imported a different Nullion source checkout')


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
def isolate_test_preferences(tmp_path, monkeypatch, _isolate_nullion_data_dir):
    from nullion import preferences
    # Consent/settings tests must never change the next test's policy.
    # Isolate units as well as product E2E, even after an earlier import.
    monkeypatch.setattr(preferences, "_PREFS_PATH", tmp_path / "preferences.json")
    yield


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


@pytest.fixture(autouse=True)
def _close_test_playwright_drivers(monkeypatch, _isolate_nullion_data_dir):
    """Each container test owns its newly launched Playwright driver processes.

    Some backend tests deliberately leave a session open. Retaining those
    drivers across thousands of cases exhausts the runner and corrupts later
    latency/browser results. Never touch borrowed or pre-existing browsers.
    """
    if not sys.platform.startswith('linux') or not pathlib.Path('/.dockerenv').exists():
        yield
        return
    try:
        from playwright._impl._transport import PipeTransport
    except ImportError:
        yield
        return
    import functools
    import signal
    import time
    drivers = []
    original = PipeTransport.connect

    def process_identity(pid):
        try:
            fields = pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
            executable = pathlib.Path(f'/proc/{pid}/exe').resolve()
            if fields[0] in {'Z', 'X'} or int(fields[1]) != os.getpid() or executable.name != 'node':
                return None
            return fields[19]
        except (OSError, ValueError, IndexError):
            return None

    @functools.wraps(original)
    async def connect(transport, *args, **kwargs):
        try:
            return await original(transport, *args, **kwargs)
        finally:
            process = getattr(transport, '_proc', None)
            pid = getattr(process, 'pid', None)
            if isinstance(pid, int) and (identity := process_identity(pid)) is not None:
                drivers.append((transport, pid, identity))
    monkeypatch.setattr(PipeTransport, 'connect', connect)
    yield
    for transport, pid, identity in drivers:
        if process_identity(pid) != identity:
            continue
        try:
            loop = getattr(transport, '_loop', None)
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(transport.request_stop)
            else:
                transport.request_stop()
        except (RuntimeError, OSError):
            pass
        deadline = time.monotonic() + 1
        while process_identity(pid) == identity and time.monotonic() < deadline:
            time.sleep(.02)
        if process_identity(pid) == identity:
            os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + 2
            while process_identity(pid) == identity and time.monotonic() < deadline:
                time.sleep(.02)
        assert process_identity(pid) != identity, 'Test-owned Playwright driver did not stop'

"""Cancellation follows a turn through real handler threads and child processes."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import threading
import time

import pytest

from nullion.tools import (ToolRegistry, ToolSpec, ToolResult, ToolInvocation,
    ToolRiskLevel, ToolSideEffectClass, tool_execution_cancellation_scope, tool_execution_cancelled)


@pytest.mark.parametrize("effect", [ToolSideEffectClass.READ, ToolSideEffectClass.WRITE])
def test_handler_retains_own_cancellation_context_and_resets_afterwards(effect):
    event = threading.Event(); registry = ToolRegistry()
    def handler(invocation):
        assert not tool_execution_cancelled()
        event.set()
        assert tool_execution_cancelled()
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {})
    registry.register(ToolSpec(name="observe", description="Observe", risk_level=ToolRiskLevel.LOW,
        side_effect_class=effect, requires_approval=False, timeout_seconds=1), handler)
    with tool_execution_cancellation_scope(event.is_set):
        registry.invoke(ToolInvocation("one", "observe", "web:one", {}))
    assert not tool_execution_cancelled()


def fake_sdk_worker(tmp_path):
    """Only the external SDK process is substituted; lifecycle is real."""
    marker = tmp_path / "worker.pid"
    executable = tmp_path / "external-sdk-worker"
    executable.write_text("#!/usr/bin/env python3\nimport os,time\nfrom pathlib import Path\n"
        + "Path(" + repr(str(marker)) + ").write_text(str(os.getpid()))\ntime.sleep(60)\n")
    executable.chmod(0o700)
    return executable, marker


def wait_worker(marker):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if marker.exists(): return int(marker.read_text())
        time.sleep(.02)
    raise AssertionError("External SDK worker never started")


def wait_dead(pid):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try: os.kill(pid, 0)
        except ProcessLookupError: return
        time.sleep(.02)
    raise AssertionError("Cancelled SDK worker still holds browser resources")


def test_isolated_sdk_worker_stops_within_owner_cancellation_budget(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin.browser_use_worker import run_isolated_browser_task
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    executable, marker = fake_sdk_worker(tmp_path); cancelled = threading.Event()
    def run():
        with tool_execution_cancellation_scope(cancelled.is_set):
            return run_isolated_browser_task(BrowserTask(task="Read a public page", url="https://example.com"),
                client=object(), principal_id="web:one", policy=BrowserPolicy(), timeout=60, python=str(executable))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run)
        pid = wait_worker(marker)
        started = time.monotonic(); cancelled.set()
        with pytest.raises(InterruptedError): future.result(timeout=3)
        assert time.monotonic() - started < 2
        wait_dead(pid)
    assert not tool_execution_cancelled()

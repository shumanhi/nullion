import importlib.util
import os
from pathlib import Path
import time

from nullion import builder_background


def test_e2e_teardown_finishes_real_queued_work_before_restoring_context(monkeypatch):
    path = Path(__file__).parent / "e2e" / "conftest.py"
    spec = importlib.util.spec_from_file_location("owned_e2e_lifecycle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("NULLION_RUN_E2E", "1")
    monkeypatch.setenv("NULLION_LIFECYCLE_TEST_CONTEXT", "owning-test")
    observed = []
    lifecycle = module.drain_test_builder_tasks.__wrapped__(monkeypatch, None)
    next(lifecycle)

    def delayed_external_response():
        time.sleep(0.05)
        observed.append(os.environ["NULLION_LIFECYCLE_TEST_CONTEXT"])

    assert builder_background.schedule_builder_background_task("lifecycle-contract", delayed_external_response)
    assert observed == []
    try:
        next(lifecycle)
    except StopIteration:
        pass
    assert observed == ["owning-test"]


def test_e2e_teardown_tracks_completion_when_real_worker_raises(monkeypatch):
    path = Path(__file__).parent / "e2e" / "conftest.py"
    spec = importlib.util.spec_from_file_location("owned_e2e_lifecycle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("NULLION_RUN_E2E", "1")
    lifecycle = module.drain_test_builder_tasks.__wrapped__(monkeypatch, None)
    next(lifecycle)
    finished = []

    def failure():
        finished.append(True)
        raise RuntimeError("external fake failure")

    assert builder_background.schedule_builder_background_task("lifecycle-error", failure)
    try:
        next(lifecycle)
    except StopIteration:
        pass
    assert finished == [True]

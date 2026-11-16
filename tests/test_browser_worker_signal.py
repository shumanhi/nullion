from types import SimpleNamespace

import pytest

from nullion.plugins.browser_plugin import browser_use_worker as worker


@pytest.mark.parametrize('returncode', [0, 7])
def test_exited_worker_signal_denial_cannot_replace_native_report(monkeypatch, caplog, returncode):
    monkeypatch.setattr(worker.platform, 'system', lambda: 'Darwin')
    def denied(*args): raise PermissionError(1, 'Operation not permitted')
    monkeypatch.setattr(worker.os, 'killpg', denied)
    worker._signal_worker(SimpleNamespace(pid=37, poll=lambda:returncode), force=True)
    assert 'Exited browser worker group cleanup denied' in caplog.text


def test_live_worker_signal_denial_is_not_treated_as_verified_shutdown(monkeypatch):
    monkeypatch.setattr(worker.platform, 'system', lambda: 'Darwin')
    def denied(*args): raise PermissionError(1, 'Operation not permitted')
    monkeypatch.setattr(worker.os, 'killpg', denied)
    with pytest.raises(PermissionError):
        worker._signal_worker(SimpleNamespace(pid=37, poll=lambda:None))

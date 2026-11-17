"""Updater graph with real git source changes, snapshots and rollback."""
import asyncio
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def test_post_install_health_timeout_restores_previous_git_version(tmp_path, monkeypatch):
    from nullion import updater
    source = tmp_path/'source'; source.mkdir()
    def git(*args):
        return subprocess.check_output(['git', '-C', str(source), *args], text=True).strip()
    git('init'); git('config', 'user.email', 'fixture@example.invalid'); git('config', 'user.name', 'Fixture')
    marker = source/'version.txt'; marker.write_text('old')
    git('add', '.'); git('commit', '-m', 'old'); old = git('rev-parse', 'HEAD')
    marker.write_text('new'); git('commit', '-am', 'new'); new = git('rev-parse', 'HEAD'); git('reset', '--hard', old)
    monkeypatch.setattr(updater, '_src_dir', lambda: source)
    backup = tmp_path/'backups'; backup.mkdir()
    monkeypatch.setattr(updater, '_backup_dir', lambda: backup)
    monkeypatch.setattr(updater, '_update_target', lambda _: updater.UpdateTarget(channel='release', ref=new, commit=new, label='new'))
    monkeypatch.setattr(updater, '_release_rollback_ref_for_snapshot', lambda: old)
    monkeypatch.setattr(updater, '_probe_web_status', lambda **_: True)
    # Package installer and health subprocess are external boundaries. Keep git,
    # snapshot ownership, graph transitions and actual source rollback real.
    monkeypatch.setattr(updater, '_install_updated_dependencies', lambda _: SimpleNamespace(returncode=0, stderr=''))
    monkeypatch.setattr(updater, '_verify_pdf_runtime_dependencies', lambda: None)
    monkeypatch.setattr(updater, 'fresh_post_update_migrations', lambda **_: {'warnings': []})
    monkeypatch.setattr(updater, '_restore_snapshot_requirements', lambda _: SimpleNamespace(returncode=0, stderr=''))
    def timeout(**kwargs):
        assert marker.read_text() == 'new'
        raise subprocess.TimeoutExpired('health', 45)
    monkeypatch.setattr(updater, 'fresh_health_check', timeout)
    result = asyncio.run(updater.run_update())
    assert not result.success and result.rolled_back, result
    assert marker.read_text() == 'old' and git('rev-parse', 'HEAD') == old
    assert result.steps[-1].step == 'rollback'

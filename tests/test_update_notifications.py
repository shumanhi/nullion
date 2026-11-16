import asyncio
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from nullion import update_notifications as notices, updater
from tests.test_updater_release_channel import _repo_with_release_and_unreleased_head, _git


@pytest.fixture
def release(tmp_path, monkeypatch):
    repo, old, new = _repo_with_release_and_unreleased_head(tmp_path)
    _git(repo, 'tag', 'v0.2.0')
    _git(repo, 'push', 'origin', '--tags')
    _git(repo, 'checkout', '--detach', old)
    monkeypatch.setenv('NULLION_SRC_DIR', str(repo))
    return SimpleNamespace(repo=repo, old=old, new=new,
        runtime=SimpleNamespace(checkpoint_path=tmp_path / 'runtime.db'))


def test_release_only_and_install_invalidates_cached_notice(release):
    runtime = release.runtime
    available = notices.check_available_update(runtime, now=100)
    assert available.has_update and available.latest == 'v0.2.0'
    _git(release.repo, 'checkout', '--detach', release.new)
    assert not notices.check_available_update(runtime, now=101).has_update
    _git(release.repo, 'tag', '-d', 'v0.2.0')
    _git(release.repo, 'push', 'origin', ':refs/tags/v0.2.0')
    available = notices.check_available_update(runtime, now=4000)
    assert not available.has_update and available.latest == 'v0.1.0'


def test_unreleased_main_is_not_an_update(tmp_path, monkeypatch):
    repo, old, _ = _repo_with_release_and_unreleased_head(tmp_path)
    _git(repo, 'checkout', '--detach', old)
    monkeypatch.setenv('NULLION_SRC_DIR', str(repo))
    available = notices.check_available_update(SimpleNamespace(checkpoint_path=tmp_path / 'runtime.db'))
    assert not available.has_update and available.latest == 'v0.1.0'


def test_divergent_candidate_does_not_announce_a_reset(release):
    (release.repo / 'candidate').write_text('local work')
    _git(release.repo, 'add', 'candidate')
    _git(release.repo, 'commit', '-m', 'local candidate')
    assert not notices.check_available_update(release.runtime).has_update


def test_failed_check_is_cached_and_recovers(release, monkeypatch):
    original = updater._update_target
    calls = []
    def fail(*args, **kwargs):
        calls.append(1)
        raise TimeoutError()
    monkeypatch.setattr(updater, '_update_target', fail)
    assert not notices.check_available_update(release.runtime, now=100).has_update
    assert not notices.check_available_update(release.runtime, now=101).has_update
    assert calls == [1]
    monkeypatch.setattr(updater, '_update_target', original)
    assert notices.check_available_update(release.runtime, now=401).has_update


def test_checks_and_notice_claims_are_shared_across_workers(release, monkeypatch):
    original = updater._update_target
    calls = []
    def tracked(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(updater, '_update_target', tracked)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: notices.check_available_update(release.runtime, now=100), range(8)))
        claims = list(pool.map(lambda _: notices._claim_notice(release.runtime, 'v0.2.0', 'telegram', '77', 100), range(8)))
    assert sum(claims) == 1 and calls == [1]
    assert any(result.has_update for result in results)


@pytest.mark.parametrize('failure,expected', [('unknown', 1), ('retry', 2), ('cancel', 1), ('false', 1)])
def test_only_known_unaccepted_delivery_can_retry(release, failure, expected):
    calls = []
    async def send(target, text):
        calls.append(text)
        if len(calls) == 1:
            if failure == 'unknown': raise httpx.ReadTimeout('Accepted, reply lost')
            if failure == 'retry': raise httpx.ConnectTimeout('Never connected')
            if failure == 'cancel': raise asyncio.CancelledError()
            return False
        return True
    async def run():
        try:
            await notices.deliver_update_notice(release.runtime, channel='telegram', target='77', send=send, now=100)
        except asyncio.CancelledError:
            assert failure == 'cancel'
        assert not await notices.deliver_update_notice(release.runtime, channel='telegram', target='77', send=send, now=101)
        await notices.deliver_update_notice(release.runtime, channel='telegram', target='77', send=send, now=401)
    asyncio.run(run())
    assert len(calls) == expected


def test_disconnected_target_does_no_git_or_database_work(release, monkeypatch):
    def forbidden(*args, **kwargs): raise AssertionError('No target means no lookup')
    monkeypatch.setattr(notices, 'check_available_update', forbidden)
    assert not asyncio.run(notices.deliver_update_notice(release.runtime, channel='telegram', target='', send=forbidden))
    assert not release.runtime.checkpoint_path.exists()


def test_restart_checkpoint_new_version_and_distinct_operator_chats(release):
    from nullion.runtime import bootstrap_persistent_runtime
    runtime = bootstrap_persistent_runtime(release.runtime.checkpoint_path)
    sent = []
    async def send(target, text):
        sent.append((target, text))
        return True
    def deliver(rt, target='77', now=100):
        return asyncio.run(notices.deliver_update_notice(rt, channel='telegram', target=target, send=send, now=now))
    assert deliver(runtime)
    runtime.checkpoint(force=True)
    runtime = bootstrap_persistent_runtime(release.runtime.checkpoint_path)
    assert not deliver(runtime)
    assert deliver(runtime, target='88')
    _git(release.repo, 'tag', 'v0.3.0', release.new)
    _git(release.repo, 'push', 'origin', '--tags')
    # Retagging the same commit is not a new installable version; create a new commit.
    _git(release.repo, 'checkout', 'main')
    (release.repo / 'next').write_text('new release')
    _git(release.repo, 'add', 'next')
    _git(release.repo, 'commit', '-m', 'next')
    _git(release.repo, 'tag', 'v0.4.0')
    _git(release.repo, 'push', 'origin', 'main', '--tags')
    _git(release.repo, 'checkout', '--detach', release.old)
    assert deliver(runtime, now=4000)
    assert len(sent) == 3 and 'v0.4.0' in sent[-1][1]
    with sqlite3.connect(runtime.checkpoint_path) as db:
        assert db.execute("SELECT count(*) FROM update_release_notices WHERE status='sent'").fetchone()[0] == 3


@pytest.mark.parametrize('surface,command', [('telegram','/update'), ('slack','/nullion update'), ('discord','/update')])
def test_shared_content_uses_native_command(surface, command):
    result = notices.update_notice_text(notices.UpdateAvailability(latest='v0.2.0'), channel=surface)
    assert 'Nullion v0.2.0 is available.' in result and f'`{command}`' in result


def test_update_git_commands_have_a_timeout(monkeypatch, tmp_path):
    def run(*args, **kwargs):
        assert kwargs['timeout'] == 30
        return SimpleNamespace(returncode=0, stdout='ok', stderr='')
    monkeypatch.setattr(updater.subprocess, 'run', run)
    assert updater._git('status', cwd=tmp_path) == (0, 'ok')

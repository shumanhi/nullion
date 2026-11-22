"""Opt-in, isolated old-release upgrade using a private stage database copy.

Prepared archives and the SQLite backup live only in ignored local evidence.
The installed source, Git origin, venv and runtime are disposable containers.
No updater, installer, migration or health implementation is mocked.
"""
import json
from hashlib import sha256
import os
import re
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
import urllib.request

import pytest

EXPLICIT_INPUTS = os.environ.get('NULLION_UPDATE_REHEARSAL_INPUTS') or ''
INPUTS = Path(EXPLICIT_INPUTS or '/inputs/app/.e2e-artifacts/update-rehearsal')
pytestmark = pytest.mark.skipif(
    os.environ.get('NULLION_RUN_E2E') != '1' or (not EXPLICIT_INPUTS and not (INPUTS / 'ready').exists()),
    reason='Requires the private stage update rehearsal fixture in Docker')


def test_released_update_command_installs_candidate_and_repeats_safely(tmp_path):
    from tests.test_updater_release_channel import _git
    from tests.e2e.test_web_surface_contracts import _free_port, _open_page
    from playwright.sync_api import expect
    assert (INPUTS / 'ready').is_file(), 'Prepare the required update rehearsal inputs before running this gate'
    manifest = json.loads((INPUTS / 'manifest.json').read_text())
    expected_revision = os.environ.get('NULLION_TEST_CANDIDATE_REVISION')
    assert expected_revision and manifest['candidate_commit'] == expected_revision, 'Rebuild stale update rehearsal inputs for this candidate'
    assert set(manifest['hashes']) == {'old.tar', 'candidate.tar', 'runtime.db'}
    for name, digest in manifest['hashes'].items():
        value = sha256()
        with (INPUTS / name).open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                value.update(block)
        assert value.hexdigest() == digest, name
    old_tag, candidate_tag = manifest['old_tag'], manifest['candidate_tag']
    assert re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', old_tag)
    assert re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', candidate_tag)
    origin = tmp_path / 'origin.git'
    publish = tmp_path / 'publish'
    publish.mkdir()
    with tarfile.open(INPUTS / 'old.tar') as archive:
        archive.extractall(publish, filter='data')
    _git(publish, 'init', '--initial-branch=main')
    _git(publish, 'config', 'user.name', 'Release fixture')
    _git(publish, 'config', 'user.email', 'release@example.invalid')
    _git(publish, 'add', '.')
    _git(publish, 'commit', '-qm', 'old release')
    _git(publish, 'tag', old_tag)
    with tarfile.open(INPUTS / 'candidate.tar') as archive:
        archive.extractall(publish, filter='data')
    _git(publish, 'add', '.')
    _git(publish, 'commit', '-qm', 'candidate release')
    target = _git(publish, 'rev-parse', 'HEAD')
    _git(publish, 'tag', candidate_tag)
    _git(tmp_path, 'clone', '--bare', str(publish), str(origin))
    installed = tmp_path / 'installed'
    _git(tmp_path, 'clone', str(origin), str(installed))
    _git(installed, 'checkout', '--detach', old_tag)
    home = tmp_path / 'home'
    home.mkdir()
    checkpoint = home / 'runtime.db'
    shutil.copy2(INPUTS / 'runtime.db', checkpoint)
    env_file = home / '.env'
    # Syntactically valid fixture token exercises Telegram construction only.
    # No poller is started and no live credentials enter this disposable lane.
    env_file.write_text('NULLION_TELEGRAM_BOT_TOKEN=123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijk\n')
    env_file.chmod(0o600)
    venv = tmp_path / 'venv'
    subprocess.run(['python', '-m', 'venv', '--system-site-packages', str(venv)], check=True)
    python = str(venv / 'bin/python')
    env = {k: v for k, v in os.environ.items() if not k.startswith(('NULLION_', 'ANTHROPIC_', 'OPENAI_', 'TELEGRAM_', 'SLACK_', 'DISCORD_'))}
    env.update(HOME=str(home), NULLION_HOME=str(home), NULLION_SRC_DIR=str(installed),
        NULLION_ENV_FILE=str(env_file), NULLION_CHECKPOINT_PATH=str(checkpoint),
        PYTHONPATH=str(installed / 'src'), PIP_NO_BUILD_ISOLATION='1', PYTHONUNBUFFERED='1')
    initial = subprocess.run([python, '-m', 'pip', 'install', '--no-deps', '--no-build-isolation', '-e', str(installed)],
        env=env, capture_output=True, text=True, timeout=120)
    assert initial.returncode == 0, initial.stderr
    port = _free_port()
    env['NULLION_WEB_PORT'] = str(port)
    base = f'http://127.0.0.1:{port}'
    server_script = (
        'import uvicorn\nfrom nullion.runtime import bootstrap_persistent_runtime\n'
        'from nullion.web_app import create_app\nfrom nullion.tools import ToolRegistry\n'
        f'runtime = bootstrap_persistent_runtime({str(checkpoint)!r})\n'
        f'uvicorn.run(create_app(runtime, None, ToolRegistry()), host="127.0.0.1", port={port}, log_level="warning")\n')
    evidence = Path('.e2e-artifacts/update-rehearsal-results')
    evidence.mkdir(parents=True, exist_ok=True)
    with (evidence / 'server.log').open('w') as log:
        process = subprocess.Popen([python, '-c', server_script], env=env, cwd=installed, stdout=log, stderr=log)
        browser = playwright = None
        try:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                assert process.poll() is None, (evidence / 'server.log').read_text()[-8000:]
                try:
                    with urllib.request.urlopen(base + '/api/health', timeout=1) as reply:
                        if reply.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                pytest.fail('Old release server did not start')
            playwright, browser, page, errors = _open_page(base)
            start = time.monotonic()
            page.locator('#user-input').fill('/update')
            page.locator('#send-btn').click()
            deadline = time.monotonic() + 30
            while _git(installed, 'rev-parse', 'HEAD') != target and time.monotonic() < deadline:
                time.sleep(0.05)
            assert _git(installed, 'rev-parse', 'HEAD') == target, 'The upgrade never reached installation'
            concurrent = page.request.post(base + '/api/chat', data={
                'text': '/update', 'conversation_id': 'web:concurrent-update-rehearsal'}, timeout=10000)
            assert concurrent.status == 200, concurrent.text()
            assert 'update is already running' in concurrent.json()['text'], concurrent.text()
            bubble = page.locator('.msg.bot .bubble').last
            expect(bubble).to_contain_text(re.compile('Updated successfully|Update failed|Already up to date'), timeout=240000)
            text = bubble.inner_text()
            (evidence / 'update-reply.txt').write_text(text)
            page.screenshot(path=str(evidence / 'upgrade-result.png'), full_page=True)
            assert 'Updated successfully: ' + old_tag in text, text
            assert candidate_tag in text and '[health]' in text, text
            assert 'ignored' not in text.lower() and 'failed' not in text.lower(), text
            elapsed = time.monotonic() - start
            assert _git(installed, 'rev-parse', 'HEAD') == target
            assert (installed / 'src/nullion/updater.py').read_bytes() == (publish / 'src/nullion/updater.py').read_bytes()
            page.screenshot(path=str(evidence / 'successful-upgrade.png'), full_page=True)
            page.locator('#user-input').fill('/update')
            page.locator('#send-btn').click()
            expect(page.locator('.msg.bot .bubble').last).to_contain_text('Already up to date: ' + candidate_tag, timeout=30000)
            result = {'from': old_tag, 'to': candidate_tag, 'candidate_revision': expected_revision, 'update_seconds': elapsed,
                'target_commit': target, 'reply': text, 'concurrent_update_rejected': True,
                'stage_database_bytes': checkpoint.stat().st_size}
            (evidence / 'result.json').write_text(json.dumps(result, indent=2))
            print(result)
        finally:
            if browser:
                browser.close()
            if playwright:
                playwright.stop()
            process.terminate()
            try:
                process.wait(10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(5)

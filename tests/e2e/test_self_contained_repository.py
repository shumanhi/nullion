"""A fresh single checkout collects tests and builds its app without a sibling repo."""
from pathlib import Path
import os
import subprocess
import sys
import zipfile

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container-only workflow')
ROOT = Path(__file__).resolve().parents[2]


def test_fresh_clone_collects_local_unit_tests_without_external_repo(tmp_path):
    env = dict(os.environ, HOME=str(tmp_path/'home'), NULLION_HOME=str(tmp_path/'data'),
               NULLION_ENV_FILE=str(tmp_path/'missing.env'), NULLION_RUN_E2E='0')
    for key in ['NULLION_APP_REPO','NULLION_TEST_REPO','NULLION_TEST_REPO_SSH_KEY','PYTHONPATH']:
        env.pop(key, None)
    result = subprocess.run([sys.executable,'-m','pytest','--collect-only','-q',
        'tests/test_messaging_adapters_workflow.py'], cwd=ROOT, env=env,
        capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'test_execute_platform_reply_delivery' in result.stdout
    assert 'collected' in result.stdout


def test_public_tests_do_not_enter_installed_wheel(tmp_path):
    result = subprocess.run([sys.executable,'-m','pip','wheel','--no-deps',
        '--no-build-isolation','--wheel-dir',str(tmp_path),str(ROOT)],
        capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    wheel, = tmp_path.glob('nullion-*.whl')
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
    assert 'nullion/__init__.py' in names
    assert not any(name.startswith(('tests/','scripts/','docker/')) for name in names)

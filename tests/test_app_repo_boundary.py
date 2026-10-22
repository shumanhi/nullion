from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def test_suite_and_package_share_one_checkout():
    assert (ROOT / 'src/nullion/__init__.py').is_file()
    assert (ROOT / 'tests/e2e/conftest.py').is_file()
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text())
    assert project['tool']['pytest']['ini_options']['testpaths'] == ['tests']
    assert project['tool']['setuptools']['packages']['find']['where'] == ['src']


def test_shards_partition_the_entire_unit_suite_without_e2e():
    expected = {str(p.relative_to(ROOT)) for pattern in ('test_*.py', '*_test.py')
                for p in (ROOT/'tests').rglob(pattern) if 'e2e' not in p.relative_to(ROOT/'tests').parts}
    seen = set()
    for shard in range(1, 9):
        result = subprocess.run([sys.executable, str(ROOT/'scripts/run_test_shard.py'),
            '--shard', str(shard), '--total', '8', '--list'], capture_output=True, text=True, check=True)
        files = set(result.stdout.splitlines())
        assert files and not (files & seen)
        seen.update(files)
    assert seen == expected


def test_invalid_shard_fails_before_running_tests():
    result = subprocess.run([sys.executable, str(ROOT/'scripts/run_test_shard.py'),
        '--shard', '9', '--total', '8', '--list'], capture_output=True, text=True)
    assert result.returncode == 2

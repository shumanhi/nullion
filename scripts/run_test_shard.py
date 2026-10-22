#!/usr/bin/env python3
"""Run a deterministic unit shard from this checkout without another repository."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def selected_files(root: Path, shard: int, total: int) -> list[Path]:
    if not 1 <= shard <= total:
        raise ValueError('shard must be between 1 and total')
    tests = root / 'tests'
    files = sorted({p for pattern in ('test_*.py', '*_test.py') for p in tests.rglob(pattern)
                    if 'e2e' not in p.relative_to(tests).parts})
    return files[shard - 1::total]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shard', required=True, type=int)
    parser.add_argument('--total', required=True, type=int)
    parser.add_argument('--list', action='store_true', help='Print selected files without running them')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    try:
        files = selected_files(root, args.shard, args.total)
    except ValueError as exc:
        parser.error(str(exc))
    if not files:
        parser.error('no test files selected')
    if args.list:
        print('\n'.join(str(p.relative_to(root)) for p in files))
        return 0
    with tempfile.TemporaryDirectory(prefix='nullion-test-home-') as home:
        env = dict(os.environ, HOME=home, NULLION_HOME=str(Path(home) / '.nullion'),
                   NULLION_ENV_FILE=str(Path(home) / '.nullion/.env'), NULLION_RUN_E2E='0',
                   NULLION_APP_REPO=str(root), PYTHONPATH=str(root / 'src'),
                   COVERAGE_FILE=str(root / '.coverage'))
        return subprocess.run([sys.executable, '-m', 'coverage', 'run', '--parallel-mode',
            '--source=src/nullion', '-m', 'pytest', '-q', *map(str, files)], cwd=root, env=env).returncode


if __name__ == '__main__':
    raise SystemExit(main())

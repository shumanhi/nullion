#!/usr/bin/env python3
"""Prepare private, revision-bound archives and a read-only database snapshot."""
from __future__ import annotations

import argparse
from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
import subprocess


def digest(path: Path) -> str:
    value = sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def prepare(*, repo: Path, database: Path, output: Path, old_ref: str, target_tag: str) -> dict:
    repo, database, output = repo.resolve(), database.resolve(), output.resolve()
    output.relative_to(repo / '.e2e-artifacts')
    if not all(re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', value) for value in [old_ref, target_tag]):
        raise ValueError('Target must be a release tag such as v0.11.2')
    if not database.is_file():
        raise FileNotFoundError(database)
    if output.exists():
        raise FileExistsError('Preserve existing rehearsal evidence; choose a fresh output directory')
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()
    candidate = git('rev-parse', 'HEAD')
    previous = git('rev-parse', old_ref + '^{commit}')
    output.mkdir(parents=True, mode=0o700)
    for name, ref in [('old', previous), ('candidate', candidate)]:
        subprocess.run(['git', '-C', str(repo), 'archive', '--format=tar',
            '--output=' + str(output / (name + '.tar')), ref], check=True)
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as source:
        with closing(sqlite3.connect(output / 'runtime.db')) as target:
            source.backup(target)
    manifest = {'old_tag': old_ref, 'candidate_tag': target_tag,
        'old_commit': previous, 'candidate_commit': candidate,
        'hashes': {name: digest(output / name)
            for name in ['old.tar', 'candidate.tar', 'runtime.db']}}
    for path in output.iterdir():
        path.chmod(0o600)
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    (output / 'manifest.json').chmod(0o600)
    (output / 'ready').touch(mode=0o600)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--old-ref', default='v0.11.1')
    parser.add_argument('--target-tag', default='v0.11.2')
    args = parser.parse_args()
    result = prepare(repo=args.repo, database=args.database, output=args.output,
        old_ref=args.old_ref, target_tag=args.target_tag)
    print(json.dumps({key: result[key] for key in ['old_tag', 'candidate_tag', 'candidate_commit']}))

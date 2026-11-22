import importlib.util
from pathlib import Path
import sqlite3
import subprocess
from contextlib import closing

import pytest

spec = importlib.util.spec_from_file_location('prepare_update_rehearsal',
    Path(__file__).resolve().parents[1] / 'scripts/prepare_update_rehearsal.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_rehearsal_archives_bind_candidate_and_snapshot_literal_database_path(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()
    git('init', '--initial-branch=main')
    git('config', 'user.email', 'fixture@example.invalid')
    git('config', 'user.name', 'Fixture')
    (repo / 'source.txt').write_text('old')
    git('add', '.')
    git('commit', '-qm', 'Old')
    git('tag', 'v0.1.0')
    (repo / 'source.txt').write_text('candidate')
    git('commit', '-am', 'Candidate', '-q')
    database = tmp_path / 'stage # ? data.db'
    with closing(sqlite3.connect(database)) as conn:
        conn.execute('create table evidence (value text)')
        conn.execute("insert into evidence values ('preserved')")
        conn.commit()
    output = repo / '.e2e-artifacts' / 'rehearsal'
    result = module.prepare(repo=repo, database=database, output=output,
        old_ref='v0.1.0', target_tag='v0.1.1')
    assert result['candidate_commit'] == git('rev-parse', 'HEAD')
    assert result['old_commit'] == git('rev-parse', 'v0.1.0')
    assert (output / 'ready').is_file()
    assert set(result['hashes']) == {'old.tar', 'candidate.tar', 'runtime.db'}
    assert all(module.digest(output / name) == value for name, value in result['hashes'].items())
    with closing(sqlite3.connect((output / 'runtime.db').as_uri() + '?mode=ro', uri=True)) as conn:
        assert conn.execute('select value from evidence').fetchone()[0] == 'preserved'
    with pytest.raises(FileExistsError):
        module.prepare(repo=repo, database=database, output=output,
            old_ref='v0.1.0', target_tag='v0.1.1')


def test_rehearsal_rejects_public_output_before_writing(tmp_path):
    with pytest.raises(ValueError):
        module.prepare(repo=tmp_path, database=tmp_path/'absent.db',
            output=tmp_path/'public', old_ref='v0.1.0', target_tag='v0.1.1')
    assert not (tmp_path / 'public').exists()

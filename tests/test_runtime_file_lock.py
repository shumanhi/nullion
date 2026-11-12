import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from nullion.runtime_persistence import _runtime_store_file_lock


def test_nested_same_thread_lock_reuses_outer_ownership(tmp_path):
    path = tmp_path / 'runtime.db'
    with _runtime_store_file_lock(path, timeout_seconds=.1):
        with _runtime_store_file_lock(path, timeout_seconds=.1):
            with _runtime_store_file_lock(tmp_path / '.' / 'runtime.db', timeout_seconds=0):
                pass
    with _runtime_store_file_lock(path, timeout_seconds=0):
        pass


def test_inner_exit_does_not_unlock_other_threads(tmp_path):
    path = tmp_path / 'runtime.db'
    def attempt():
        try:
            with _runtime_store_file_lock(path, timeout_seconds=.05):
                return True
        except sqlite3.OperationalError:
            return False
    with ThreadPoolExecutor(max_workers=1) as pool:
        with _runtime_store_file_lock(path):
            with _runtime_store_file_lock(path, timeout_seconds=.1):
                pass
            assert pool.submit(attempt).result(timeout=2) is False
        assert pool.submit(attempt).result(timeout=2) is True


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='POSIX fork ownership')
def test_fork_does_not_inherit_reentrant_ownership(tmp_path):
    path = tmp_path / 'runtime.db'
    with _runtime_store_file_lock(path):
        pid = os.fork()
        if pid == 0:
            try:
                with _runtime_store_file_lock(path, timeout_seconds=.05):
                    os._exit(2)
            except sqlite3.OperationalError:
                os._exit(0)
            except BaseException:
                os._exit(3)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0


def test_exception_releases_reentrant_ownership(tmp_path):
    path = tmp_path / 'runtime.db'
    with pytest.raises(ValueError):
        with _runtime_store_file_lock(path):
            with _runtime_store_file_lock(path, timeout_seconds=.1):
                raise ValueError('rollback')
    with _runtime_store_file_lock(path, timeout_seconds=0):
        pass

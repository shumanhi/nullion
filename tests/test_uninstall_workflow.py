from __future__ import annotations

import errno
import shutil

from nullion import cli


def test_remove_path_retries_directory_not_empty_race(monkeypatch, tmp_path) -> None:
    target = tmp_path / "nullion-home"
    target.mkdir()
    (target / "late.log").write_text("still flushing", encoding="utf-8")
    real_rmtree = shutil.rmtree
    calls = {"count": 0}

    def flaky_rmtree(path, *args, **kwargs):
        if path == target and calls["count"] == 0:
            calls["count"] += 1
            raise OSError(errno.ENOTEMPTY, "Directory not empty", str(path))
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(cli.shutil, "rmtree", flaky_rmtree)
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)

    message = cli._remove_path(target, dry_run=False)

    assert message == f"Removed {target}"
    assert not target.exists()
    assert calls["count"] == 1


def test_remove_path_keeps_dry_run_directory(tmp_path) -> None:
    target = tmp_path / "nullion-home"
    target.mkdir()
    (target / "runtime-store.json").write_text("{}", encoding="utf-8")

    message = cli._remove_path(target, dry_run=True)

    assert message == f"Would remove {target}"
    assert target.exists()
    assert (target / "runtime-store.json").exists()

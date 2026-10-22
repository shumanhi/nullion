from __future__ import annotations

import errno
import os
import shutil
from types import SimpleNamespace

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


def test_remove_path_schedules_windows_retry_for_locked_tree(monkeypatch, tmp_path) -> None:
    target = tmp_path / "nullion-home"
    target.mkdir()
    (target / "locked.pyd").write_text("locked", encoding="utf-8")
    scheduled: list[object] = []

    def locked_rmtree(_path, *args, **kwargs):
        raise PermissionError(errno.EACCES, "Access is denied", str(target / "locked.pyd"))

    monkeypatch.setattr(cli, "os", SimpleNamespace(name="nt", getpid=os.getpid))
    monkeypatch.setattr(cli.shutil, "rmtree", locked_rmtree)
    monkeypatch.setattr(cli, "_schedule_windows_tree_removal", lambda path: scheduled.append(path))

    message = cli._remove_path(target, dry_run=False)

    assert message == f"Scheduled removal of {target}"
    assert scheduled == [target]


def test_kill_existing_webview_uses_taskkill_on_windows(monkeypatch, tmp_path) -> None:
    home = tmp_path / ".nullion"
    home.mkdir()
    pid_file = home / "webview.pid"
    pid_file.write_text("12345", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run(args, **kwargs):
        calls.append(list(args))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli, "_NULLION_HOME", home)
    monkeypatch.setattr(cli, "os", SimpleNamespace(name="nt", environ=os.environ, getpid=os.getpid))
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    cli._kill_existing_webview(pid_file=pid_file)

    assert calls == [["taskkill", "/PID", "12345", "/T", "/F"]]
    assert not pid_file.exists()


def test_remove_launchd_services_does_not_bootout_unowned_labels(monkeypatch, tmp_path) -> None:
    launch_agents = tmp_path / "Library" / "LaunchAgents"
    calls: list[list[str]] = []

    monkeypatch.setattr(
        cli,
        "_launchd_plist_for_label",
        lambda label: launch_agents / f"{label}.plist",
    )
    monkeypatch.setattr(cli.shutil, "which", lambda command: f"/bin/{command}")
    monkeypatch.setattr(
        cli,
        "_run_uninstall_subprocess",
        lambda command, *, dry_run: calls.append(list(command)),
    )

    actions = cli._remove_launchd_services(dry_run=False)

    assert actions == []
    assert calls == []


def test_remove_launchd_services_only_boots_out_owned_labels(monkeypatch, tmp_path) -> None:
    launch_agents = tmp_path / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True)
    tray_plist = launch_agents / "com.nullion.tray.plist"
    tray_plist.write_text("plist", encoding="utf-8")
    calls: list[list[str]] = []

    monkeypatch.setattr(
        cli,
        "_launchd_plist_for_label",
        lambda label: launch_agents / f"{label}.plist",
    )
    monkeypatch.setattr(cli.shutil, "which", lambda command: f"/bin/{command}")
    monkeypatch.setattr(cli.os, "getuid", lambda: 501)
    monkeypatch.setattr(
        cli,
        "_run_uninstall_subprocess",
        lambda command, *, dry_run: calls.append(list(command)),
    )

    actions = cli._remove_launchd_services(dry_run=False)

    assert calls == [
        ["launchctl", "bootout", "gui/501/com.nullion.tray"],
        ["launchctl", "unload", str(tray_plist)],
    ]
    assert actions == [f"Removed {tray_plist}"]
    assert not tray_plist.exists()

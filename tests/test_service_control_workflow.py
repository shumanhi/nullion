from __future__ import annotations

import subprocess

from nullion import service_control


def test_launchd_restart_treats_running_agent_as_success_after_kickstart_warning(monkeypatch, tmp_path) -> None:
    plist = tmp_path / "com.nullion.web.plist"
    plist.write_text("<plist/>", encoding="utf-8")
    calls: list[list[str]] = []
    print_calls = 0

    def fake_plist_for_label(label: str):
        assert label == "com.nullion.web"
        return plist

    def fake_run(args: list[str], *, timeout: float = 15.0):  # noqa: ARG001
        nonlocal print_calls
        calls.append(args)
        if args[:2] == ["launchctl", "bootout"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:2] == ["launchctl", "bootstrap"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:3] == ["launchctl", "kickstart", "-k"]:
            return subprocess.CompletedProcess(args, 5, stdout="", stderr="unknown launchctl error")
        if args[:2] == ["launchctl", "print"]:
            print_calls += 1
            if print_calls == 1:
                return subprocess.CompletedProcess(args, 1, stdout="", stderr="not loaded")
            return subprocess.CompletedProcess(args, 0, stdout="state = running\npid = 4436\n", stderr="")
        if args[:2] == ["ps", "-axo"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(service_control, "launchd_plist_for_label", fake_plist_for_label)
    monkeypatch.setattr(service_control, "_run", fake_run)
    monkeypatch.setattr(service_control.os, "getuid", lambda: 501)

    message = service_control.restart_managed_service("web", manager="launchd")

    assert message == "Restarted the Web service (com.nullion.web)."
    assert ["launchctl", "print", "gui/501/com.nullion.web"] in calls
    assert ["launchctl", "bootout", "gui/501/com.nullion.web"] in calls


def test_launchd_restart_retries_bootstrap_for_unloaded_agent(monkeypatch, tmp_path) -> None:
    plist = tmp_path / "ai.nullion.telegram.plist"
    plist.write_text("<plist/>", encoding="utf-8")
    calls: list[list[str]] = []
    bootstrap_calls = 0

    def fake_plist_for_label(label: str):
        assert label == "ai.nullion.telegram"
        return plist

    def fake_run(args: list[str], *, timeout: float = 15.0):  # noqa: ARG001
        nonlocal bootstrap_calls
        calls.append(args)
        if args[:2] == ["launchctl", "bootout"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:2] == ["launchctl", "bootstrap"]:
            bootstrap_calls += 1
            if bootstrap_calls == 1:
                return subprocess.CompletedProcess(args, 5, stdout="", stderr="Bootstrap failed")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:3] == ["launchctl", "kickstart", "-k"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:2] == ["launchctl", "print"]:
            if bootstrap_calls < 2:
                return subprocess.CompletedProcess(args, 1, stdout="", stderr="not loaded")
            return subprocess.CompletedProcess(args, 0, stdout="state = running\npid = 4436\n", stderr="")
        if args[:2] == ["ps", "-axo"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(service_control, "launchd_plist_for_label", fake_plist_for_label)
    monkeypatch.setattr(service_control, "_run", fake_run)
    monkeypatch.setattr(service_control.os, "getuid", lambda: 501)

    message = service_control.restart_managed_service("telegram", manager="launchd")

    assert message == "Restarted the Telegram service (ai.nullion.telegram)."
    assert bootstrap_calls == 2


def test_launchd_restart_kills_stale_tray_and_webview_processes(monkeypatch, tmp_path) -> None:
    plist = tmp_path / "com.nullion.tray.plist"
    plist.write_text("<plist/>", encoding="utf-8")
    home = tmp_path / "home"
    tray = home / "venv" / "bin" / "nullion-tray"
    python = home / "venv" / "bin" / "python"
    calls: list[list[str]] = []
    killed: list[list[str]] = []
    ps_remaining = True
    loaded = False

    def fake_run(args: list[str], *, timeout: float = 15.0):  # noqa: ARG001
        nonlocal loaded, ps_remaining
        calls.append(args)
        if args[:2] == ["launchctl", "bootout"]:
            loaded = False
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:2] == ["launchctl", "bootstrap"]:
            loaded = True
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:3] == ["launchctl", "kickstart", "-k"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:2] == ["launchctl", "print"]:
            if not loaded:
                return subprocess.CompletedProcess(args, 1, stdout="", stderr="not loaded")
            return subprocess.CompletedProcess(args, 0, stdout="state = running\npid = 999\n", stderr="")
        if args[:2] == ["ps", "-axo"]:
            stdout = ""
            if ps_remaining:
                stdout = (
                    f"111 {tray} --port 8742\n"
                    f"112 {python} -m nullion.webview_app --port 8742\n"
                )
            return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")
        if args[:1] == ["kill"]:
            killed.append(args)
            ps_remaining = False
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setenv("NULLION_HOME", str(home))
    monkeypatch.setattr(service_control, "launchd_plist_for_label", lambda label: plist)
    monkeypatch.setattr(service_control, "_run", fake_run)
    monkeypatch.setattr(service_control.os, "getuid", lambda: 501)

    message = service_control.restart_managed_service("tray", manager="launchd")

    assert message == "Restarted the Tray service (com.nullion.tray)."
    assert ["kill", "-TERM", "111"] in killed
    assert ["kill", "-TERM", "112"] in killed

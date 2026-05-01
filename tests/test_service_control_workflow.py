from __future__ import annotations

import subprocess

from nullion import service_control


def test_launchd_restart_treats_running_agent_as_success_after_kickstart_warning(monkeypatch, tmp_path) -> None:
    plist = tmp_path / "com.nullion.web.plist"
    plist.write_text("<plist/>", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_plist_for_label(label: str):
        assert label == "com.nullion.web"
        return plist

    def fake_run(args: list[str], *, timeout: float = 15.0):  # noqa: ARG001
        calls.append(args)
        if args[:2] == ["launchctl", "list"]:
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:3] == ["launchctl", "kickstart", "-k"]:
            return subprocess.CompletedProcess(args, 5, stdout="", stderr="unknown launchctl error")
        if args[:2] == ["launchctl", "print"]:
            return subprocess.CompletedProcess(args, 0, stdout="state = running\npid = 4436\n", stderr="")
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(service_control, "launchd_plist_for_label", fake_plist_for_label)
    monkeypatch.setattr(service_control, "_run", fake_run)
    monkeypatch.setattr(service_control.os, "getuid", lambda: 501)

    message = service_control.restart_managed_service("web", manager="launchd")

    assert message == "Restarted the Web service (com.nullion.web)."
    assert ["launchctl", "print", "gui/501/com.nullion.web"] in calls

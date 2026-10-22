from __future__ import annotations

import sys
from types import SimpleNamespace


def test_desktop_entrypoint_starts_tray_and_webview(monkeypatch, capsys) -> None:
    from nullion import cli

    tray_ports: list[int] = []
    popen_commands: list[list[str]] = []

    monkeypatch.setattr(cli.sys, "platform", "darwin")
    monkeypatch.setattr(cli, "_install_tray_autostart", lambda *, port, start: tray_ports.append(port) or True)
    monkeypatch.setattr(
        cli.subprocess,
        "Popen",
        lambda command, **kwargs: popen_commands.append(command) or SimpleNamespace(pid=1234),
    )

    cli._open_desktop_entrypoint(port=9123, force_reload=False)

    assert tray_ports == [9123]
    assert popen_commands
    command_head = popen_commands[0][0]
    assert (
        popen_commands[0][:3] == [cli.sys.executable, "-m", "nullion.webview_app"]
        or command_head.endswith("nullion-webview")
    )
    assert "--port" in popen_commands[0]
    assert "9123" in popen_commands[0]
    assert "--browser-fallback" in popen_commands[0]
    assert "Opened Web UI." in capsys.readouterr().out


def test_desktop_entrypoint_falls_back_to_browser_when_webview_launch_fails(monkeypatch, capsys) -> None:
    from nullion import cli

    browser_urls: list[str] = []

    monkeypatch.setattr(cli.sys, "platform", "darwin")
    monkeypatch.setattr(cli, "_install_tray_autostart", lambda *, port, start: True)
    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("blocked")))
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda command, **kwargs: browser_urls.append(command[-1]) or SimpleNamespace(returncode=0),
    )

    cli._open_desktop_entrypoint(port=9124, force_reload=False)

    assert browser_urls == ["http://localhost:9124"]
    assert "Opened Web UI fallback: http://localhost:9124" in capsys.readouterr().out


def test_nullion_dashboard_flag_opens_desktop_entrypoint(monkeypatch) -> None:
    from nullion import cli

    opened: list[tuple[int, bool]] = []

    monkeypatch.setattr(sys, "argv", ["nullion", "--dashboard"])
    monkeypatch.setattr(cli, "_default_web_port", lambda: 9191)
    monkeypatch.setattr(
        cli,
        "_open_desktop_entrypoint",
        lambda *, port, force_reload: opened.append((port, force_reload)),
    )

    cli._nullion_ctl_impl()

    assert opened == [(9191, False)]

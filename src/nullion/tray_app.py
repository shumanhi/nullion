"""Nullion tray/menu-bar companion.

The tray app is intentionally a thin local client.  The background web service
owns runtime state and privileged operations; this process just gives people a
native-ish place to open the Web UI, check health, and request restarts.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from typing import Any

from nullion.entrypoint_guard import run_single_instance_entrypoint, run_user_facing_entrypoint


_DEFAULT_PORT = 8742
_DEFAULT_HOST = "127.0.0.1"
_NULLION_HOME = Path.home() / ".nullion"
_ENV_FILE = _NULLION_HOME / ".env"
_LOG_DIR = _NULLION_HOME / "logs"


def _load_env(env_file: str | None) -> None:
    path = Path(env_file).expanduser() if env_file else _ENV_FILE
    if not path.exists():
        return
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _web_base_url(host: str, port: int) -> str:
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{port}"


def _open_path(path: Path) -> None:
    path = path.expanduser()
    if sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    elif os.name == "nt":
        os.startfile(str(path))  # type: ignore[attr-defined]
    else:
        subprocess.Popen(["xdg-open", str(path)])


def _request_json(url: str, *, method: str = "GET", timeout: float = 2.5) -> dict[str, Any]:
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload or "{}")
    return data if isinstance(data, dict) else {}


def _short_status(status_payload: dict[str, Any] | None) -> str:
    if not status_payload:
        return "Offline"
    if status_payload.get("status") == "ok" and "packages" in status_payload:
        return "Running"
    summary = status_payload.get("summary")
    if isinstance(summary, dict):
        approvals = int(summary.get("pending_approvals") or summary.get("approval_required") or 0)
        running = int(summary.get("running_missions") or summary.get("running") or 0)
        if approvals:
            return f"Running, {approvals} approval{'s' if approvals != 1 else ''} waiting"
        if running:
            return f"Running, {running} active task{'s' if running != 1 else ''}"
    return "Running"


def _build_icon_image(online: bool = False, *, size: int = 64):
    try:
        from PIL import Image, ImageDraw
    except ImportError as exc:
        raise RuntimeError("Pillow is required for the Nullion tray icon. Install with: pip install pillow") from exc

    render_size = max(size * 4, 256)
    scale = render_size / 64

    def box(values: tuple[int, int, int, int]) -> tuple[int, int, int, int]:
        return tuple(round(value * scale) for value in values)

    def width(value: int) -> int:
        return max(1, round(value * scale))

    image = Image.new("RGBA", (render_size, render_size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    # High-resolution robot source; downsampled for a crisp tray icon and reused
    # directly for launcher/app switcher contexts.
    draw.rounded_rectangle(box((10, 16, 54, 56)), radius=round(13 * scale), fill=(87, 70, 230, 255), outline=(42, 31, 126, 255), width=width(3))
    draw.rounded_rectangle(box((17, 23, 47, 48)), radius=round(8 * scale), fill=(250, 252, 255, 255))
    draw.line((round(32 * scale), round(16 * scale), round(32 * scale), round(8 * scale)), fill=(250, 252, 255, 255), width=width(4))
    draw.ellipse(box((27, 3, 37, 13)), fill=(250, 252, 255, 255), outline=(42, 31, 126, 255), width=width(2))
    draw.rounded_rectangle(box((5, 29, 12, 43)), radius=round(3 * scale), fill=(87, 70, 230, 255), outline=(42, 31, 126, 255), width=width(2))
    draw.rounded_rectangle(box((52, 29, 59, 43)), radius=round(3 * scale), fill=(87, 70, 230, 255), outline=(42, 31, 126, 255), width=width(2))
    draw.ellipse(box((22, 31, 29, 38)), fill=(29, 27, 45, 255))
    draw.ellipse(box((35, 31, 42, 38)), fill=(29, 27, 45, 255))
    draw.rounded_rectangle(box((25, 43, 39, 47)), radius=round(2 * scale), fill=(128, 116, 255, 255))

    badge = (34, 211, 153, 255) if online else (154, 153, 170, 255)
    draw.ellipse(box((45, 45, 61, 61)), fill=badge, outline=(11, 11, 13, 255), width=width(2))
    if render_size == size:
        return image
    return image.resize((size, size), Image.Resampling.LANCZOS)


class NullionTray:
    def __init__(self, *, host: str, port: int, poll_interval: float) -> None:
        self.host = host
        self.port = port
        self.poll_interval = poll_interval
        self.base_url = _web_base_url(host, port)
        self.status_label = "Checking..."
        self.online = False
        self._stop = threading.Event()
        self.icon = None
        self._mac_handler = None
        self._webview_process: subprocess.Popen | None = None

    def run(self) -> None:
        if sys.platform == "darwin":
            self._run_macos_status_item()
            return
        try:
            import pystray
        except ImportError as exc:
            raise RuntimeError("pystray is required for the Nullion tray icon. Install with: pip install pystray") from exc

        self.pystray = pystray
        self.icon = pystray.Icon("nullion", _build_icon_image(False), "Nullion", self._menu())
        threading.Thread(target=self._poll_loop, name="nullion-tray-health", daemon=True).start()
        self.icon.run()

    def _menu(self):
        Menu = self.pystray.Menu
        MenuItem = self.pystray.MenuItem
        return Menu(
            MenuItem(f"Status: {self.status_label}", None, enabled=False),
            Menu.SEPARATOR,
            MenuItem("Open Web UI", self.open_web_ui, default=True),
            MenuItem("Open Approvals", self.open_approvals),
            MenuItem("Open in Browser", self.open_browser),
            MenuItem("Open Logs", self.open_logs),
            MenuItem("Open Config", self.open_config),
            Menu.SEPARATOR,
            MenuItem("Restart Nullion", self.restart_nullion),
            MenuItem("Restart Chat Services", self.restart_chat_services),
            Menu.SEPARATOR,
            MenuItem("Quit Tray Icon", self.quit),
        )

    def _refresh_menu(self) -> None:
        if self._mac_handler is not None:
            self._mac_handler.update_status()
            return
        if not self.icon:
            return
        self.icon.title = f"Nullion - {self.status_label}"
        self.icon.icon = _build_icon_image(self.online)
        self.icon.menu = self._menu()
        self.icon.update_menu()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self.refresh_status()
            self._stop.wait(self.poll_interval)

    def refresh_status(self) -> None:
        try:
            health = _request_json(f"{self.base_url}/api/health")
            if health.get("status") != "ok":
                raise RuntimeError("health check did not return ok")
            try:
                status = _request_json(f"{self.base_url}/api/status")
            except Exception:
                status = health
            self.online = True
            self.status_label = _short_status(status)
        except Exception:
            self.online = False
            self.status_label = "Offline"
        self._refresh_menu()

    def notify(self, message: str) -> None:
        if not self.icon:
            return
        try:
            self.icon.notify(message, "Nullion")
        except Exception:
            pass

    def open_web_ui(self, *_args) -> None:
        self._open_webview()

    def open_browser(self, *_args) -> None:
        webbrowser.open(self.base_url)

    def open_approvals(self, *_args) -> None:
        self._open_webview("#approvals")

    def _open_webview(self, path: str = "") -> None:
        if self._webview_process is not None and self._webview_process.poll() is None:
            if sys.platform == "darwin":
                subprocess.run(
                    [
                        "osascript",
                        "-e",
                        f'tell application "System Events" to set frontmost of first process whose unix id is {self._webview_process.pid} to true',
                    ],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            return
        command = [
            sys.executable,
            "-m",
            "nullion.webview_app",
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--browser-fallback",
        ]
        if path:
            command.extend(["--path", path])
        env_file = os.environ.get("NULLION_ENV_FILE")
        if env_file:
            command.extend(["--env-file", env_file])
        try:
            self._webview_process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            webbrowser.open(f"{self.base_url}{path}")

    def open_logs(self, *_args) -> None:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        _open_path(_LOG_DIR)

    def open_config(self, *_args) -> None:
        if not _ENV_FILE.exists():
            _ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
            _ENV_FILE.touch()
        _open_path(_ENV_FILE)

    def restart_nullion(self, *_args) -> None:
        try:
            _request_json(f"{self.base_url}/api/restart", method="POST", timeout=4.0)
            self.notify("Restart requested.")
        except urllib.error.URLError:
            self._run_nullion_shortcut("--restart")
        except Exception as exc:
            self.notify(f"Restart failed: {exc}")
        finally:
            threading.Thread(target=self._delayed_refresh, daemon=True).start()

    def restart_chat_services(self, *_args) -> None:
        try:
            data = _request_json(f"{self.base_url}/api/chat-services/restart", method="POST", timeout=8.0)
            self.notify(str(data.get("message") or "Chat services restart requested."))
        except Exception as exc:
            self.notify(f"Chat service restart failed: {exc}")
        finally:
            self.refresh_status()

    def _run_nullion_shortcut(self, flag: str) -> None:
        try:
            subprocess.Popen(["nullion", flag], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.notify(f"Ran nullion {flag}.")
        except Exception as exc:
            self.notify(f"Could not run nullion {flag}: {exc}")

    def _delayed_refresh(self) -> None:
        time.sleep(4)
        self.refresh_status()

    def quit(self, *_args) -> None:
        self._stop.set()
        if self._mac_handler is not None:
            self._mac_handler.quit_(None)
            return
        if self.icon:
            self.icon.stop()

    def _run_macos_status_item(self) -> None:
        try:
            import AppKit
            import Foundation
            import objc
        except ImportError as exc:
            raise RuntimeError("PyObjC/AppKit is required for the macOS tray icon.") from exc

        tray = self

        class _MacTrayHandler(Foundation.NSObject):
            def init(self):
                self = objc.super(_MacTrayHandler, self).init()
                if self is None:
                    return None
                self.status_item = None
                self.status_menu = None
                self.status_menu_item = None
                return self

            def activate_(self, sender):
                event = AppKit.NSApp.currentEvent()
                event_type = event.type() if event is not None else None
                flags = event.modifierFlags() if event is not None else 0
                control_flag = getattr(AppKit, "NSEventModifierFlagControl", getattr(AppKit, "NSControlKeyMask", 1 << 18))
                if event_type == AppKit.NSRightMouseUp or flags & control_flag:
                    self.status_item.popUpStatusItemMenu_(self.status_menu)
                else:
                    tray.open_web_ui()

            def openWebUI_(self, sender):
                tray.open_web_ui()

            def openApprovals_(self, sender):
                tray.open_approvals()

            def openBrowser_(self, sender):
                tray.open_browser()

            def openLogs_(self, sender):
                tray.open_logs()

            def openConfig_(self, sender):
                tray.open_config()

            def restartNullion_(self, sender):
                tray.restart_nullion()

            def restartChatServices_(self, sender):
                tray.restart_chat_services()

            def refresh_(self, sender):
                tray.refresh_status()

            def quit_(self, sender):
                tray._stop.set()
                AppKit.NSApp.terminate_(None)

            def update_status(self):
                if self.status_item is None:
                    return
                button = self.status_item.button()
                button.setToolTip_(f"Nullion - {tray.status_label}")
                button.setImage_(_mac_image(tray.online))
                if self.status_menu_item is not None:
                    self.status_menu_item.setTitle_(f"Status: {tray.status_label}")

        def _mac_image(online: bool):
            image = _build_icon_image(online)
            buffer = io.BytesIO()
            image.save(buffer, format="PNG")
            raw = buffer.getvalue()
            data = Foundation.NSData.dataWithBytes_length_(raw, len(raw))
            ns_image = AppKit.NSImage.alloc().initWithData_(data)
            ns_image.setSize_(Foundation.NSMakeSize(20, 20))
            return ns_image

        def _item(title: str, action: str | None = None, enabled: bool = True):
            item = AppKit.NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(title, action, "")
            item.setEnabled_(enabled)
            if action:
                item.setTarget_(handler)
            return item

        app = AppKit.NSApplication.sharedApplication()
        app.setActivationPolicy_(AppKit.NSApplicationActivationPolicyAccessory)
        handler = _MacTrayHandler.alloc().init()
        self._mac_handler = handler
        status_item = AppKit.NSStatusBar.systemStatusBar().statusItemWithLength_(AppKit.NSVariableStatusItemLength)
        handler.status_item = status_item
        button = status_item.button()
        button.setTarget_(handler)
        button.setAction_("activate:")
        button.sendActionOn_(AppKit.NSLeftMouseUpMask | AppKit.NSRightMouseUpMask)

        menu = AppKit.NSMenu.alloc().init()
        handler.status_menu = menu
        handler.status_menu_item = _item(f"Status: {self.status_label}", enabled=False)
        menu.addItem_(handler.status_menu_item)
        menu.addItem_(AppKit.NSMenuItem.separatorItem())
        menu.addItem_(_item("Open Web UI", "openWebUI:"))
        menu.addItem_(_item("Open Approvals", "openApprovals:"))
        menu.addItem_(_item("Open in Browser", "openBrowser:"))
        menu.addItem_(_item("Open Logs", "openLogs:"))
        menu.addItem_(_item("Open Config", "openConfig:"))
        menu.addItem_(AppKit.NSMenuItem.separatorItem())
        menu.addItem_(_item("Restart Nullion", "restartNullion:"))
        menu.addItem_(_item("Restart Chat Services", "restartChatServices:"))
        menu.addItem_(_item("Refresh Status", "refresh:"))
        menu.addItem_(AppKit.NSMenuItem.separatorItem())
        menu.addItem_(_item("Quit Tray Icon", "quit:"))

        handler.update_status()
        self.refresh_status()
        Foundation.NSTimer.scheduledTimerWithTimeInterval_target_selector_userInfo_repeats_(
            max(self.poll_interval, 2.0),
            handler,
            "refresh:",
            None,
            True,
        )
        app.run()


def cli() -> None:
    return run_user_facing_entrypoint(_cli_impl)


def _cli_impl() -> None:
    parser = argparse.ArgumentParser(prog="nullion-tray", description="Nullion tray/menu-bar companion")
    parser.add_argument("--host", default=os.environ.get("NULLION_WEB_HOST", _DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("NULLION_WEB_PORT", _DEFAULT_PORT)))
    parser.add_argument("--env-file", default=os.environ.get("NULLION_ENV_FILE"))
    parser.add_argument("--poll-interval", type=float, default=10.0)
    args = parser.parse_args()

    _load_env(args.env_file)
    return run_single_instance_entrypoint(
        "tray",
        lambda: _run_tray(args),
        wait_seconds=1.0,
        description="nullion-tray",
    )


def _run_tray(args) -> None:
    tray = NullionTray(host=args.host, port=args.port, poll_interval=max(args.poll_interval, 2.0))
    try:
        tray.run()
    except RuntimeError as exc:
        print(f"nullion-tray: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cli()

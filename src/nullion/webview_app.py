"""Native webview shell for the local Nullion Web UI."""
from __future__ import annotations

import argparse
import atexit
import os
from pathlib import Path
import subprocess
import sys
import webbrowser

from nullion.entrypoint_guard import run_single_instance_entrypoint, run_user_facing_entrypoint
from nullion.tray_app import _DEFAULT_HOST, _DEFAULT_PORT, _load_env, _web_base_url, _build_icon_image


_NULLION_HOME = Path.home() / ".nullion"
_WEBVIEW_PID_FILE = _NULLION_HOME / "webview.pid"


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _focus_process(pid: int) -> bool:
    if not _pid_is_running(pid):
        return False
    if sys.platform == "darwin":
        script = f'tell application "System Events" to set frontmost of first process whose unix id is {pid} to true'
        return subprocess.run(["osascript", "-e", script], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    return True


def _claim_single_instance() -> bool:
    if _WEBVIEW_PID_FILE.exists():
        try:
            existing_pid = int(_WEBVIEW_PID_FILE.read_text().strip())
        except ValueError:
            existing_pid = 0
        if existing_pid and _focus_process(existing_pid):
            return False
    _WEBVIEW_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    _WEBVIEW_PID_FILE.write_text(f"{os.getpid()}\n")

    def cleanup() -> None:
        try:
            if _WEBVIEW_PID_FILE.read_text().strip() == str(os.getpid()):
                _WEBVIEW_PID_FILE.unlink()
        except Exception:
            pass

    atexit.register(cleanup)
    return True


def _webview_icon_path() -> str:
    icon_dir = _NULLION_HOME / "icons"
    icon_dir.mkdir(parents=True, exist_ok=True)
    image = _build_icon_image(True, size=512)
    if os.name == "nt":
        path = icon_dir / "nullion-robot.ico"
        image.save(path, format="ICO", sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64)])
    else:
        path = icon_dir / "nullion-robot.png"
        image.save(path, format="PNG")
    return str(path)


def _set_macos_app_icon(icon_path: str) -> None:
    if sys.platform != "darwin":
        return
    try:
        import AppKit

        image = AppKit.NSImage.alloc().initWithContentsOfFile_(icon_path)
        if image is not None:
            AppKit.NSApplication.sharedApplication().setApplicationIconImage_(image)
    except Exception:
        pass


def _resolve_window_size(webview_module) -> tuple[int, int]:
    """Pick a sensible default window size that scales with the user's display.

    Resolution order:
      1. Explicit env vars NULLION_WEBVIEW_WIDTH / NULLION_WEBVIEW_HEIGHT.
      2. ~85% of the primary screen, clamped to a [1280x800, 1920x1200] range
         so it's generous on big monitors but never larger than the screen
         on a 13" laptop.
      3. Fallback (1440x900) if pywebview can't tell us the screen size.
    """
    # Explicit override wins.
    try:
        env_w = int(os.environ.get("NULLION_WEBVIEW_WIDTH", "") or 0)
        env_h = int(os.environ.get("NULLION_WEBVIEW_HEIGHT", "") or 0)
    except ValueError:
        env_w = env_h = 0
    if env_w > 0 and env_h > 0:
        return env_w, env_h

    fallback = (1440, 900)
    try:
        screens = list(webview_module.screens or [])
    except Exception:
        screens = []
    if not screens:
        return fallback

    # Use the primary (first) screen's dimensions.
    s = screens[0]
    sw = int(getattr(s, "width", 0) or 0)
    sh = int(getattr(s, "height", 0) or 0)
    if sw <= 0 or sh <= 0:
        return fallback

    max_width = min(1920, sw)
    max_height = min(1200, sh)
    width = min(max(int(sw * 0.85), min(1280, max_width)), max_width)
    height = min(max(int(sh * 0.85), min(800, max_height)), max_height)
    return width, height


def cli() -> None:
    return run_user_facing_entrypoint(_cli_impl)


def _cli_impl() -> None:
    parser = argparse.ArgumentParser(prog="nullion-webview", description="Open Nullion in a native webview window")
    parser.add_argument("--host", default=os.environ.get("NULLION_WEB_HOST", _DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("NULLION_WEB_PORT", _DEFAULT_PORT)))
    parser.add_argument("--env-file", default=os.environ.get("NULLION_ENV_FILE"))
    parser.add_argument("--path", default="", help="Path or hash to append to the Web UI URL, e.g. #approvals")
    parser.add_argument("--debug", action="store_true", help="Enable webview debug mode")
    parser.add_argument("--browser-fallback", action="store_true", help="Open the default browser if webview is unavailable")
    parser.add_argument("--width", type=int, default=None, help="Window width in pixels (overrides auto-sizing)")
    parser.add_argument("--height", type=int, default=None, help="Window height in pixels (overrides auto-sizing)")
    args = parser.parse_args()

    _load_env(args.env_file)
    url = _web_base_url(args.host, args.port)
    if args.path:
        suffix = args.path if args.path.startswith(("/", "#", "?")) else f"/{args.path}"
        url = f"{url}{suffix}"

    try:
        import webview
    except ImportError:
        if args.browser_fallback:
            webbrowser.open(url)
            return
        print("nullion-webview: pywebview is not installed. Install with: pip install pywebview", file=sys.stderr)
        sys.exit(1)

    return run_single_instance_entrypoint(
        "webview",
        lambda: _run_webview(args, webview, url),
        wait_seconds=0.0,
        description="nullion-webview",
    )


def _run_webview(args, webview, url: str) -> None:
    if not _claim_single_instance():
        return

    icon_path = _webview_icon_path()
    _set_macos_app_icon(icon_path)

    # CLI flags override auto / env. Auto-size scales with the user's screen
    # so the default is generous on big monitors and safe on small ones.
    if args.width and args.height:
        width, height = args.width, args.height
    else:
        width, height = _resolve_window_size(webview)

    webview.create_window(
        "Nullion",
        url,
        width=width,
        height=height,
        min_size=(920, 620),
        text_select=True,
    )
    webview.start(debug=args.debug, icon=icon_path)


if __name__ == "__main__":
    cli()

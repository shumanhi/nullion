"""Native webview shell for the local Nullion Web UI."""
from __future__ import annotations

import argparse
import atexit
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import quote
from urllib.parse import urljoin
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen
import webbrowser

from nullion.entrypoint_guard import run_single_instance_entrypoint, run_user_facing_entrypoint
from nullion.tray_app import _DEFAULT_HOST, _DEFAULT_PORT, _load_env as _load_tray_env, _web_base_url, _build_icon_image
from nullion.version import version_tag


_DEFAULT_NULLION_HOME = Path.home() / ".nullion"


def _configured_nullion_home() -> Path:
    configured = os.environ.get("NULLION_HOME")
    if configured and configured.strip():
        return Path(configured).expanduser()
    return _DEFAULT_NULLION_HOME.expanduser()


class _DynamicNullionHome(os.PathLike[str]):
    def __fspath__(self) -> str:
        return str(_configured_nullion_home())

    def __str__(self) -> str:
        return str(Path(self))

    def __getattr__(self, name: str):
        return getattr(Path(self), name)

    def __truediv__(self, key: str) -> Path:
        return Path(self) / key

    def __eq__(self, other: object) -> bool:
        try:
            return Path(self) == Path(other)  # type: ignore[arg-type]
        except TypeError:
            return False

    def __repr__(self) -> str:
        return repr(Path(self))


class _DynamicWebviewPidFile(os.PathLike[str]):
    def __fspath__(self) -> str:
        return str(_configured_nullion_home() / "webview.pid")

    def __str__(self) -> str:
        return str(Path(self))

    def __getattr__(self, name: str):
        return getattr(Path(self), name)

    def __eq__(self, other: object) -> bool:
        try:
            return Path(self) == Path(other)  # type: ignore[arg-type]
        except TypeError:
            return False

    def __repr__(self) -> str:
        return repr(Path(self))


_NULLION_HOME = _DynamicNullionHome()
_WEBVIEW_PID_FILE: os.PathLike[str] | Path | None = _DynamicWebviewPidFile()


def _env_web_host() -> str:
    return os.environ.get("NULLION_WEB_HOST", _DEFAULT_HOST)


def _env_web_port() -> int:
    try:
        return int(os.environ.get("NULLION_WEB_PORT", _DEFAULT_PORT))
    except (TypeError, ValueError):
        return _DEFAULT_PORT


def _resolve_target_args(args) -> None:
    if args.host is None:
        args.host = _env_web_host()
    if args.port is None:
        args.port = _env_web_port()


def _nullion_home() -> Path:
    return _configured_nullion_home()


def _webview_pid_file() -> Path:
    if _WEBVIEW_PID_FILE is not None:
        return Path(_WEBVIEW_PID_FILE).expanduser()
    return _nullion_home() / "webview.pid"


def _load_env(env_file: str | None) -> None:
    _load_tray_env(env_file, override=bool(env_file) or bool(os.environ.get("NULLION_HOME")))


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
    pid_file = _webview_pid_file()
    if pid_file.exists():
        try:
            existing_pid = int(pid_file.read_text().strip())
        except ValueError:
            existing_pid = 0
        if existing_pid and _focus_process(existing_pid):
            return False
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(f"{os.getpid()}\n")

    def cleanup() -> None:
        try:
            if pid_file.read_text().strip() == str(os.getpid()):
                pid_file.unlink()
        except Exception:
            pass

    atexit.register(cleanup)
    return True


def _webview_icon_path() -> str:
    icon_dir = _nullion_home() / "icons"
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


def _avatar_data_url() -> str:
    path = Path(__file__).resolve().parent / "assets" / "nullion-assistant-avatar.svg"
    try:
        svg = path.read_text(encoding="utf-8")
    except OSError:
        return ""
    return f"data:image/svg+xml;charset=utf-8,{quote(svg)}"


def _webview_version_tag() -> str:
    try:
        return str(version_tag() or "").strip()
    except Exception:
        return ""


def _restart_all_services_for_loading_screen() -> dict[str, object]:
    try:
        from nullion.service_control import restart_managed_services

        results = restart_managed_services(continue_on_error=True)
        payload = [
            {"service": result.service, "ok": result.ok, "message": result.message}
            for result in results
        ]
        ok_count = sum(1 for result in results if result.ok)
        web_result = next((result for result in results if result.service == "web"), None)
        if web_result is not None and not web_result.ok:
            return {
                "ok": False,
                "message": web_result.message,
                "results": payload,
            }
        if ok_count:
            return {
                "ok": True,
                "message": f"Restart requested for {ok_count} installed service{'s' if ok_count != 1 else ''}.",
                "results": payload,
            }
        if payload:
            return {
                "ok": False,
                "message": "No installed Nullion services could be restarted.",
                "results": payload,
            }
    except Exception as exc:
        service_error = str(exc)
    else:
        service_error = "No installed Nullion services were found."

    try:
        subprocess.Popen(["nullion", "--restart"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:
        return {
            "ok": False,
            "message": f"Restart failed: {service_error}; fallback command failed: {exc}",
            "results": [],
        }
    return {
        "ok": True,
        "message": "Restart command launched.",
        "results": [{"service": "nullion", "ok": True, "message": "Ran nullion --restart."}],
    }


def _check_loading_screen_health(base_url: str) -> dict[str, object]:
    health_url = urljoin(base_url, "/api/health")
    request = Request(health_url, headers={"User-Agent": "NullionWebview/health"})
    try:
        with urlopen(request, timeout=2) as response:
            status = getattr(response, "status", 200)
            return {
                "ok": 200 <= int(status) < 300,
                "status": int(status),
                "message": "Local app is healthy.",
            }
    except HTTPError as exc:
        return {
            "ok": False,
            "status": int(exc.code),
            "message": f"Health check returned HTTP {exc.code}.",
        }
    except (TimeoutError, URLError, OSError) as exc:
        reason = getattr(exc, "reason", None) or str(exc)
        return {
            "ok": False,
            "status": 0,
            "message": f"Cannot reach the local app: {reason}",
        }


class _LoadingScreenApi:
    def __init__(self, target_url: str = "") -> None:
        self._target_url = target_url

    def health(self) -> dict[str, object]:
        if not self._target_url:
            return {"ok": False, "status": 0, "message": "No local app URL is configured."}
        return _check_loading_screen_health(self._target_url)

    def restart_services(self) -> dict[str, object]:
        return _restart_all_services_for_loading_screen()


def _loading_html(url: str) -> str:
    target = json.dumps(url)
    avatar = json.dumps(_avatar_data_url())
    version = json.dumps(_webview_version_tag())
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nullion</title>
  <style>
    html, body {{ height: 100%; margin: 0; background: #0f1117; color: #eef2ff; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    body {{ display: grid; place-items: center; }}
    main {{ display: grid; gap: 18px; justify-items: center; transform: translateY(-6px); }}
    .logo-wrap {{ position: relative; width: 82px; height: 82px; display: grid; place-items: center; }}
    .logo-wrap::before {{ content: ""; position: absolute; inset: -6px; border-radius: 50%; border: 3px solid rgba(125, 107, 255, .18); border-top-color: #7d6bff; animation: spin 1.05s linear infinite; }}
    .logo {{ width: 72px; height: 72px; border-radius: 50%; object-fit: cover; display: block; box-shadow: 0 18px 44px rgba(0,0,0,.34); }}
    .fallback-mark {{ width: 42px; height: 42px; border-radius: 50%; border: 3px solid rgba(125, 107, 255, .25); border-top-color: #7d6bff; animation: spin 1s linear infinite; }}
    .title {{ font-size: 31px; font-weight: 760; letter-spacing: 0; line-height: 1.1; }}
    .actions {{ position: fixed; right: 28px; bottom: 24px; display: grid; gap: 10px; justify-items: end; }}
    .restart {{ height: 38px; padding: 0 18px; border: 1px solid rgba(167,139,250,.42); border-radius: 999px; background: rgba(124,106,255,.15); color: #d9d2ff; font: inherit; font-size: 13px; font-weight: 760; box-shadow: 0 10px 28px rgba(0,0,0,.22); cursor: pointer; }}
    .restart.attention {{ border-color: rgba(251,113,133,.66); color: #ffe4e6; background: rgba(127,29,29,.34); }}
    .restart:disabled {{ opacity: .62; cursor: default; }}
    .progress {{ width: 230px; display: grid; gap: 7px; opacity: .88; }}
    .progress-label {{ text-align: center; font-size: 12px; font-weight: 650; color: #8e96a8; min-height: 16px; }}
    .progress-track {{ height: 4px; overflow: hidden; border-radius: 999px; background: rgba(255,255,255,.08); }}
    .progress-fill {{ width: 18%; height: 100%; border-radius: inherit; background: linear-gradient(90deg, #7d6bff, #38bdf8); box-shadow: 0 0 16px rgba(125,107,255,.44); transition: width .25s ease; }}
    .version-tag {{ width: max-content; max-width: min(360px, calc(100vw - 48px)); margin: 2px auto 0; text-align: center; font-size: 10px; font-weight: 650; color: #9aa3b5; letter-spacing: 0; line-height: 1.2; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-height: 12px; }}
    .status-card {{ width: min(330px, calc(100vw - 44px)); margin-top: 2px; padding: 12px 14px; border-radius: 8px; border: 1px solid rgba(248,113,113,.42); background: rgba(69,10,10,.34); box-shadow: 0 18px 44px rgba(0,0,0,.28); }}
    .status-title {{ font-size: 13px; font-weight: 760; color: #fecaca; line-height: 1.25; }}
    .status-detail {{ margin-top: 4px; font-size: 12px; font-weight: 620; color: #c7cedd; line-height: 1.35; }}
    .status-card[data-tone="warn"] {{ border-color: rgba(251,191,36,.42); background: rgba(69,45,10,.30); }}
    .status-card[data-tone="warn"] .status-title {{ color: #fde68a; }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
  </style>
</head>
<body>
  <main>
    <div class="logo-wrap" aria-hidden="true" id="logo-wrap"></div>
    <div class="title">Starting Nullion</div>
    <div class="progress" aria-hidden="true">
      <div class="progress-label" id="progress-label">Checking the local app...</div>
      <div class="progress-track"><div class="progress-fill" id="progress-fill"></div></div>
      <div class="version-tag" id="version-tag"></div>
    </div>
    <div class="status-card" id="status-card" data-tone="warn" hidden>
      <div class="status-title" id="status-title">Still waiting</div>
      <div class="status-detail" id="status-detail">The local app has not answered yet.</div>
    </div>
  </main>
  <div class="actions">
    <button class="restart" id="restart-btn" type="button">Restart services</button>
  </div>
  <script>
    const target = {target};
    const avatar = {avatar};
    const version = {version};
    const healthUrl = new URL('/api/health', target).toString();
    const restartUrl = new URL('/api/restart', target).toString();
    const logoWrap = document.getElementById('logo-wrap');
    const progressLabel = document.getElementById('progress-label');
    const progressFill = document.getElementById('progress-fill');
    const versionTag = document.getElementById('version-tag');
    const statusCard = document.getElementById('status-card');
    const statusTitle = document.getElementById('status-title');
    const statusDetail = document.getElementById('status-detail');
    const restartBtn = document.getElementById('restart-btn');
    let pollDelay = 450;
    let attempt = 0;
    let redirected = false;
    let nativeApi = null;
    let nativeApiPromise = null;

    logoWrap.innerHTML = avatar
      ? `<img class="logo" src="${{avatar}}" alt="">`
      : '<div class="fallback-mark"></div>';
    if (versionTag && version) versionTag.textContent = `Version ${{version}}`;

    function setProgress(text, pct) {{
      progressLabel.textContent = text;
      progressFill.style.width = `${{Math.max(8, Math.min(100, pct))}}%`;
    }}

    function showStatus(title, detail, tone = 'warn') {{
      statusTitle.textContent = title;
      statusDetail.textContent = detail;
      statusCard.dataset.tone = tone;
      statusCard.hidden = false;
      if (tone === 'error') restartBtn.classList.add('attention');
    }}

    function hideStatus() {{
      statusCard.hidden = true;
      restartBtn.classList.remove('attention');
    }}

    async function waitForNativeApi() {{
      if (nativeApi) return nativeApi;
      if (nativeApiPromise) return nativeApiPromise;
      nativeApiPromise = (async () => {{
      for (let i = 0; i < 40; i += 1) {{
        if (window.pywebview && window.pywebview.api && window.pywebview.api.restart_services) {{
          nativeApi = window.pywebview.api;
          return nativeApi;
        }}
        await new Promise(resolve => setTimeout(resolve, 100));
      }}
      return null;
      }})();
      const resolved = await nativeApiPromise;
      if (!resolved) nativeApiPromise = null;
      return resolved;
    }}

    async function checkHealth() {{
      const api = await waitForNativeApi();
      if (api && api.health) {{
        return api.health();
      }}
      try {{
        const response = await fetch(healthUrl, {{ cache: 'no-store' }});
        return {{
          ok: response.ok,
          status: response.status,
          message: response.ok ? 'Local app is healthy.' : `Health check returned HTTP ${{response.status}}.`,
        }};
      }} catch (error) {{
        return {{
          ok: false,
          status: 0,
          message: error && error.message ? `Cannot reach the local app: ${{error.message}}` : 'Cannot reach the local app.',
        }};
      }}
    }}

    async function pollHealth() {{
      if (redirected) return;
      attempt += 1;
      setProgress(attempt < 4 ? 'Checking the local app...' : 'Still waiting for the local app...', Math.min(82, 12 + attempt * 6));
      try {{
        const result = await checkHealth();
        if (result && result.ok) {{
          redirected = true;
          hideStatus();
          setProgress('Connected. Opening Nullion...', 100);
          window.location.replace(target);
          return;
        }}
        if (attempt >= 8) {{
          const detail = result && result.message ? result.message : 'The local app has not answered yet.';
          showStatus('Local app is not responding', detail, attempt >= 14 ? 'error' : 'warn');
          if (attempt >= 14) {{
            setProgress('Startup appears stuck.', 92);
          }}
        }}
      }} catch (_error) {{
        if (attempt >= 8) {{
          showStatus('Local app is not responding', 'The health check failed before Nullion opened.', attempt >= 14 ? 'error' : 'warn');
        }}
      }}
      pollDelay = Math.min(1800, Math.round(pollDelay * 1.18));
      setTimeout(pollHealth, pollDelay);
    }}

    async function restartServices() {{
      restartBtn.disabled = true;
      restartBtn.textContent = 'Restarting...';
      hideStatus();
      setProgress('Stopping and restarting services...', 44);
      try {{
        const api = await waitForNativeApi();
        if (api) {{
          const result = await api.restart_services();
          if (!result || result.ok === false) throw new Error((result && result.message) || 'Restart failed');
        }} else {{
          const response = await fetch(restartUrl, {{ method: 'POST', cache: 'no-store' }});
          if (!response.ok) throw new Error('Restart request failed');
        }}
        setProgress('Restart requested. Waiting for the app...', 62);
        attempt = 0;
        pollDelay = 500;
        setTimeout(pollHealth, 800);
      }} catch (error) {{
        const detail = error && error.message ? error.message : 'Restart failed.';
        showStatus('Restart failed', detail, 'error');
        setProgress('Restart failed.', 100);
        restartBtn.disabled = false;
        restartBtn.textContent = 'Try restart again';
      }}
    }}

    restartBtn.addEventListener('click', restartServices);
    setTimeout(pollHealth, 120);
  </script>
</body>
</html>"""


def cli() -> None:
    return run_user_facing_entrypoint(_cli_impl)


def _cli_impl() -> None:
    parser = argparse.ArgumentParser(prog="nullion-webview", description="Open Nullion in a native webview window")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--env-file", default=os.environ.get("NULLION_ENV_FILE"))
    parser.add_argument("--path", default="", help="Path or hash to append to the Web UI URL, e.g. #approvals")
    parser.add_argument("--debug", action="store_true", help="Enable webview debug mode")
    parser.add_argument("--browser-fallback", action="store_true", help="Open the default browser if webview is unavailable")
    parser.add_argument("--width", type=int, default=None, help="Window width in pixels (overrides auto-sizing)")
    parser.add_argument("--height", type=int, default=None, help="Window height in pixels (overrides auto-sizing)")
    args = parser.parse_args()

    _load_env(args.env_file)
    _resolve_target_args(args)
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
        html=_loading_html(url),
        width=width,
        height=height,
        min_size=(920, 620),
        text_select=True,
        js_api=_LoadingScreenApi(url),
    )
    webview.start(debug=args.debug, icon=icon_path)


if __name__ == "__main__":
    cli()

from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run slow install-to-web e2e tests",
)


ROOT = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run(
    args: list[str],
    *,
    env: dict[str, str],
    cwd: Path = ROOT,
    timeout: float = 120.0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=True,
    )


def _json_request(url: str, *, payload: dict | None = None, timeout: float = 10.0) -> dict:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method="POST" if payload is not None else "GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_for_health(base_url: str, process: subprocess.Popen[str], *, timeout: float = 45.0) -> dict:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout is not None else ""
            raise AssertionError(f"web app exited before becoming healthy:\n{output[-4000:]}")
        try:
            payload = _json_request(f"{base_url}/api/health", timeout=2.0)
            if payload.get("status") == "ok":
                return payload
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        time.sleep(0.5)
    raise AssertionError(f"web app did not become healthy: {last_error!r}")


def test_editable_install_can_launch_web_and_exercise_major_http_paths(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    venv = tmp_path / "venv"
    port = _free_port()
    env_file = home / ".nullion" / ".env"
    env_file.parent.mkdir(parents=True)
    env_file.write_text(
        "\n".join(
            [
                "NULLION_MODEL_PROVIDER=openai",
                "NULLION_MODEL=e2e-model",
                "OPENAI_API_KEY=sk-e2e-not-used",
                "NULLION_KEY_STORAGE=local",
                f"NULLION_WEB_PORT={port}",
                "NULLION_ENABLED_PLUGINS=workspace_plugin",
                f"NULLION_WORKSPACE_ROOT={workspace}",
                f"NULLION_ALLOWED_ROOTS={workspace}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    env = {
        **os.environ,
        "HOME": str(home),
        "NULLION_ENV_FILE": str(env_file),
        "NULLION_DATA_DIR": str(home / ".nullion"),
        "PYTHONUNBUFFERED": "1",
    }

    _run([sys.executable, "-m", "venv", str(venv)], env=env)
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    scripts = venv / ("Scripts" if os.name == "nt" else "bin")

    _run([str(python), "-m", "pip", "install", "--upgrade", "pip"], env=env, timeout=180)
    _run([str(python), "-m", "pip", "install", "-e", str(ROOT)], env=env, timeout=300)

    for script in ["nullion", "nullion-cli", "nullion-auth", "nullion-web"]:
        exe = scripts / (f"{script}.exe" if os.name == "nt" else script)
        assert exe.exists(), f"missing console script: {exe}"
        _run([str(exe), "--help"], env=env, timeout=30)

    checkpoint = home / ".nullion" / "runtime.db"
    web = subprocess.Popen(
        [
            str(scripts / ("nullion-web.exe" if os.name == "nt" else "nullion-web")),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--env-file",
            str(env_file),
            "--checkpoint",
            str(checkpoint),
        ],
        cwd=str(ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        base_url = f"http://127.0.0.1:{port}"
        health = _wait_for_health(base_url, web)
        assert health["status"] == "ok"

        status = _json_request(f"{base_url}/api/status")
        assert isinstance(status, dict)
        assert any(key in status for key in ("summary", "approvals", "tools", "runtime"))

        config = _json_request(f"{base_url}/api/config")
        assert config.get("model_provider") in {
            "openai",
            "codex",
            "anthropic",
            "custom",
            "openrouter",
            "gemini",
            "groq",
            "mistral",
            "deepseek",
            "xai",
            "together",
            "ollama",
        }

        preferences = _json_request(f"{base_url}/api/preferences")
        assert isinstance(preferences, dict)

        profile = _json_request(
            f"{base_url}/api/profile",
            payload={"name": "E2E Operator", "email": "e2e@example.test", "ignored": "value"},
        )
        assert profile["ok"] is True

        chat = _json_request(
            f"{base_url}/api/chat",
            payload={"text": "/status", "conversation_id": "web:e2e", "stream": False},
        )
        assert chat["type"] == "message"
        assert "status" in chat["text"].lower() or "no active" in chat["text"].lower()
    finally:
        web.terminate()
        try:
            web.wait(timeout=10)
        except subprocess.TimeoutExpired:
            web.kill()
            web.wait(timeout=10)

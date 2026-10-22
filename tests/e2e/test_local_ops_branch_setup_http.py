from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E only")


def test_branch_setup_http_persists_only_branch_pins(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[2] / "scripts/local_ops_dashboard.py"
    spec = importlib.util.spec_from_file_location("ops_http_contract", script)
    dashboard = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = dashboard
    spec.loader.exec_module(dashboard)
    for key, attribute in (("app", "APP_REPO"), ("test", "ROOT")):
        repo = tmp_path / key
        subprocess.run(["git", "init", "--initial-branch=main", str(repo)], check=True, capture_output=True)
        for command in (("config", "user.name", "Test"), ("config", "user.email", "test@example.com"),
                        ("commit", "--allow-empty", "-m", "initial")):
            subprocess.run(["git", "-C", str(repo), *command], check=True, capture_output=True)
        monkeypatch.setattr(dashboard, attribute, repo)
    monkeypatch.chdir(tmp_path / "app")
    state = tmp_path / "pins.json"
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", state)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        payload = {"action": "create_paired_branches", "new_paired_branch": "branch-contract",
                   "base_branch": "local:main", "test_base_branch": "local:main"}
        request = Request(base + "/api/action", data=json.dumps(payload).encode(),
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=10) as response:
            result = json.load(response)
        assert result["ok"] is True, result
        assert set(json.loads(state.read_text())) == {"app", "test"}
        assert set(result["summary"]["details"]) == {"app_branch", "test_branch"}
        assert "bug_tracker" not in json.dumps(result)
        assert "qa" not in result["raw"]
        assert not list(tmp_path.rglob("*.xlsx"))
        assert not list(tmp_path.rglob("*.numbers"))
        with urlopen(base, timeout=10) as response:
            page = response.read().decode()
        assert "Pairs app and nullion-test branches" in page
        assert "Pinned QA file" not in page
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)

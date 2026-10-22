from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import replace
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
    repo = tmp_path / "app"
    subprocess.run(["git", "init", "--initial-branch=main", str(repo)], check=True, capture_output=True)
    for command in (("config", "user.name", "Test"), ("config", "user.email", "test@example.com"),
                    ("commit", "--allow-empty", "-m", "initial")):
        subprocess.run(["git", "-C", str(repo), *command], check=True, capture_output=True)
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "ROOT", repo)
    monkeypatch.chdir(repo)
    state = tmp_path / "pins.json"
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", state)
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        payload = {"action": "create_work_branch", "new_branch": "branch-contract",
                   "base_branch": "local:main"}
        request = Request(base + "/api/action", data=json.dumps(payload).encode(),
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=10) as response:
            result = json.load(response)
        assert result["ok"] is True, result
        assert set(json.loads(state.read_text())) == {"app"}
        assert set(result["summary"]["details"]) == {"branch"}
        assert "bug_tracker" not in json.dumps(result)
        assert "qa" not in result["raw"]
        assert not list(tmp_path.rglob("*.xlsx"))
        assert not list(tmp_path.rglob("*.numbers"))
        with urlopen(base, timeout=10) as response:
            page = response.read().decode()
        assert "One branch for app, tests, and tools" in page
        assert "Pinned QA file" not in page
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


def test_dashboard_ui_runs_selected_branch_code_and_tests_without_other_repo(tmp_path, monkeypatch):
    from playwright.sync_api import sync_playwright, expect

    script = Path(__file__).resolve().parents[2] / "scripts/local_ops_dashboard.py"
    spec = importlib.util.spec_from_file_location("ops_single_repo_ui", script)
    dashboard = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = dashboard
    spec.loader.exec_module(dashboard)
    repo = tmp_path / "app"
    repo.mkdir()
    def git(*args):
        return subprocess.run(["git", "-C", str(repo), *args], check=True,
                              capture_output=True, text=True).stdout.strip()
    git("init", "--initial-branch=main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    (repo / "scripts").mkdir()
    (repo / "tests").mkdir()
    (repo / "src/nullion").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[tool.pytest.ini_options]\n")
    runner = repo / "scripts/e2e-docker.sh"
    runner.write_text("#!/bin/sh\nset -eu\nexec python -m pytest -q -s tests\n")
    runner.chmod(0o755)
    marker = repo / "src/nullion/marker.txt"
    test = repo / "tests/test_selected.py"
    def write_suite(value):
        marker.write_text(value)
        test.write_text("from pathlib import Path\ndef test_selected():\n"
                        f" assert Path('src/nullion/marker.txt').read_text() == {value!r}\n"
                        f" print({value!r})\n")
    write_suite("selected-branch-suite")
    git("add", ".")
    git("commit", "-m", "initial")
    git("update-ref", "refs/remotes/origin/main", "HEAD")
    monkeypatch.setattr(dashboard, "ROOT", repo)
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", tmp_path / "pins.json")
    monkeypatch.setattr(dashboard, "DEV_WORKTREE_DIR", tmp_path / "worktrees")
    monkeypatch.setattr(dashboard, "DEV", replace(dashboard.DEV, install_dir=tmp_path / "runtime"))
    server = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.locator("summary").filter(has_text="Create work branch").click()
            page.get_by_label("New work branch", exact=True).fill("unidad-work")
            page.get_by_role("button", name="Create and pin", exact=True).click()
            expect(page.locator("#pinStatus")).to_contain_text("nullion/unidad-work", timeout=15000)
            assert set(json.loads(dashboard.PINNED_BRANCH_FILE.read_text())) == {"app"}
            assert git("rev-parse", "nullion/unidad-work") == git("rev-parse", "main")
            # Diverge the dashboard's own checkout: mixing its suite with the
            # selected branch's app would now fail or print the wrong receipt.
            write_suite("wrong-main-suite")
            git("add", ".")
            git("commit", "-m", "different main suite")
            page.get_by_label("Dev runtime branch picker").select_option("local:nullion/unidad-work")
            page.get_by_role("button", name="Run E2E on selected branch").click()
            expect(page.locator("#actionOutput")).to_contain_text("E2E started", timeout=15000)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                jobs = list(dashboard.JOBS.values())
                if jobs and jobs[-1]["status"] != "running":
                    break
                time.sleep(0.1)
            assert jobs[-1]["status"] == "passed", jobs
            selected = Path(jobs[-1]["cwd"])
            assert selected != repo
            output = Path(jobs[-1]["log_path"]).read_text()
            assert "selected-branch-suite" in output and "wrong-main-suite" not in output
            assert (selected / "src/nullion/marker.txt").read_text() == "selected-branch-suite"
            assert not (tmp_path / "other-repository").exists()
            expect(page.locator("#pinStatus")).not_to_contain_text("test branch")
            browser.close()
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)

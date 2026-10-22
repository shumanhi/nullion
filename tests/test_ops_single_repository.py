from pathlib import Path
import json
import plistlib
import sys

import pytest

from test_local_ops_dashboard_pins import _git, _init_repo, _load_dashboard


def test_workspace_location_is_shared_by_git_worktrees(tmp_path):
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    checkout = tmp_path / "worktrees" / "feature"
    _git(repo, "worktree", "add", "--detach", str(checkout), "main")
    assert dashboard.checkout_workspace(repo) == tmp_path
    assert dashboard.checkout_workspace(checkout) == tmp_path


def test_test_job_uses_both_code_and_tests_from_selected_checkout(tmp_path, monkeypatch):
    dashboard = _load_dashboard()
    selected = tmp_path / "selected"
    (selected / "scripts").mkdir(parents=True)
    (selected / "tests").mkdir()
    runner = selected / "scripts/e2e-docker.sh"
    runner.write_text("#!/bin/sh\nexit 0\n")
    monkeypatch.setattr(dashboard, "_resolve_dev_source", lambda selection: {"ok": True, "path": selected})
    calls = []
    monkeypatch.setattr(dashboard, "_start_background_job", lambda *args, **kw: calls.append((args, kw)) or {"ok": True})
    assert dashboard.start_e2e("local:feature", "tests/e2e/example.py")["ok"]
    args, kwargs = calls[0]
    assert args == ("e2e", [str(runner), "tests/e2e/example.py"])
    assert kwargs == {"cwd": selected, "env": {"NULLION_APP_REPO": str(selected)}}
    runner.unlink()
    assert not dashboard.start_e2e("local:feature", "")["ok"]
    assert len(calls) == 1


@pytest.mark.parametrize("fails", [False, True])
def test_dashboard_installer_preserves_configuration_and_only_restarts_dashboard(tmp_path, monkeypatch, fails):
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    for name in ("src/nullion", "tests", "scripts"):
        (repo / name).mkdir(parents=True)
    (repo / "pyproject.toml").write_text("")
    (repo / "scripts/e2e-docker.sh").write_text("")
    agents = tmp_path / "agents"
    agents.mkdir()
    plist = agents / f"{dashboard.LOCAL_OPS_LAUNCHD_LABEL}.plist"
    before = plistlib.dumps({"Label": dashboard.LOCAL_OPS_LAUNCHD_LABEL, "ProgramArguments": ["old"]})
    plist.write_bytes(before)
    monkeypatch.setattr(dashboard, "ROOT", repo)
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "HOME", tmp_path)
    monkeypatch.setattr(dashboard, "LAUNCH_AGENTS_DIR", agents)
    monkeypatch.setattr(dashboard.sys, "platform", "darwin")
    calls = []
    def launchctl(args, **kw):
        calls.append(args)
        return {"ok": not (fails and len(calls) == 2), "stderr": "fixture load failure" if fails else ""}
    monkeypatch.setattr(dashboard, "_run", launchctl)
    result = dashboard.install_dashboard_launchagent()
    assert result["ok"] is not fails
    assert Path(result["backup"]).read_bytes() == before
    assert all(args[0] == "launchctl" for args in calls)
    assert calls[0][-1].endswith("/" + dashboard.LOCAL_OPS_LAUNCHD_LABEL)
    assert all(not any(name in str(args) for name in ("com.nullion.web", "com.nullion.stage", "ai.nullion.telegram")) for args in calls)
    if fails:
        assert plist.read_bytes() == before
    else:
        payload = plistlib.loads(plist.read_bytes())
        assert payload["ProgramArguments"][1] == str(repo / "scripts/local_ops_dashboard.py")
        assert payload["WorkingDirectory"] == str(repo)
        assert payload["EnvironmentVariables"]["NULLION_APP_REPO"] == str(repo)
        assert payload["StandardOutPath"].startswith(str(tmp_path / ".nullion-ops"))


def test_rejected_legacy_test_actions_cannot_create_a_second_branch(tmp_path, monkeypatch):
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", tmp_path / "pins.json")
    before = _git(repo, "branch", "--format=%(refname)")
    for action in ("create_test_branch", "pin_test_branch", "create_paired_branches"):
        assert not dashboard.action(action, {"new_branch": "unused"})["ok"]
    assert _git(repo, "branch", "--format=%(refname)") == before
    assert not (tmp_path / "pins.json").exists()


def test_orphan_detection_stays_within_selected_instance(tmp_path, monkeypatch):
    dashboard = _load_dashboard()
    from dataclasses import replace
    dev = replace(dashboard.DEV, install_dir=tmp_path / "dev")
    prod = tmp_path / "prod"
    command = f"{dev.venv_python} -m nullion.web_app --port {dev.port}"
    output = f"111 {command}\n222 {prod}/venv/bin/python -m nullion.web_app --port 8742\n333 /bin/echo other\n"
    monkeypatch.setattr(dashboard, "_run", lambda *a, **kw: {"ok": True, "stdout": output})
    assert dashboard._instance_service_pids(dev) == ["111"]


def test_cleanup_preserves_dashboard_source_and_pinned_branch_after_merge(tmp_path, monkeypatch):
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    checkout = tmp_path / "dashboard"
    _git(repo, "worktree", "add", "-b", "dashboard-source", str(checkout), "main")
    _git(repo, "branch", "pinned-work", "main")
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "ROOT", checkout)
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", tmp_path / "pins.json")
    dashboard.pin_work_branch("local:pinned-work")
    for branch in ("dashboard-source", "pinned-work"):
        assert not dashboard.cleanup_branch("local:" + branch)["ok"]
    result = dashboard.cleanup_merged_branches()
    assert result["ok"] and set(result["removed"]) == {"nullion/existing"}
    assert (checkout / ".git").is_file()
    assert _git(repo, "rev-parse", "pinned-work") == _git(repo, "rev-parse", "main")

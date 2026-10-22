from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def _load_dashboard():
    path = Path(__file__).resolve().parents[1] / "scripts" / "local_ops_dashboard.py"
    spec = importlib.util.spec_from_file_location("local_ops_dashboard", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _dashboard_script() -> Path:
    return Path(__file__).resolve().parents[1] / "scripts" / "local_ops_dashboard.py"


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _init_repo(repo: Path) -> None:
    repo.mkdir()
    subprocess.run(["git", "init", "--initial-branch=main", str(repo)], check=True, capture_output=True)
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test User")
    (repo / "README.md").write_text("test\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "init")
    _git(repo, "branch", "nullion/existing")


def _commit(repo: Path, message: str, *, date: str) -> None:
    env = {
        **os.environ,
        "GIT_AUTHOR_DATE": date,
        "GIT_COMMITTER_DATE": date,
    }
    subprocess.run(["git", "-C", str(repo), "commit", "--allow-empty", "-m", message], check=True, capture_output=True, env=env)


def test_branch_rows_include_all_local_heads_sorted_by_update(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    _git(repo, "switch", "-c", "backup/local-only")
    _commit(repo, "older local", date="2026-01-01T00:00:00+0000")
    _git(repo, "switch", "-c", "nullion/recent")
    _commit(repo, "newer local", date="2026-01-02T00:00:00+0000")
    monkeypatch.setattr(dashboard, "APP_REPO", repo)

    rows = dashboard.branch_rows(repo)
    values = [row["value"] for row in rows]

    assert "local:backup/local-only" in values
    assert values.index("local:nullion/recent") < values.index("local:backup/local-only")


def test_stage_picker_prefers_managed_origin_source_when_commits_match() -> None:
    script = _dashboard_script().read_text(encoding="utf-8")
    match = re.search(r"function branchForInstance\(.*?\n}\n\nfunction sourceForProdInstance", script, re.S)
    assert match
    function_source = match.group(0).removesuffix("\n\nfunction sourceForProdInstance")
    node_script = f"""
let lastData = {{
  branches: [
    {{source: "local", branch: "nullion/work", value: "local:nullion/work", commit: "abc1234"}},
    {{source: "origin", branch: "main", value: "origin:main", commit: "abc1234"}}
  ]
}};
{function_source}
const selected = branchForInstance({{
  key: "stage",
  expected_source: "/stage-main",
  source_matches_expected: true,
  source_git: {{path: "/stage-main", branch: "", commit: "abc123456"}}
}}, "origin:main");
if (selected !== "origin:main") {{
  throw new Error(`expected origin:main, got ${{selected}}`);
}}
"""
    subprocess.run(["node", "-e", node_script], check=True, capture_output=True, text=True)


def test_prod_sources_include_local_branches_but_reset_can_refuse_them(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    _git(repo, "tag", "v1.2.3")
    monkeypatch.setattr(dashboard, "APP_REPO", repo)

    branches = dashboard.branch_rows(repo)
    releases = dashboard.release_tag_rows(repo)
    prod_sources = dashboard._sorted_prod_source_rows(branches, releases)

    assert prod_sources
    assert "release" in {row["source"] for row in prod_sources}
    assert "local:nullion/existing" in {row["value"] for row in prod_sources}
    assert dashboard._resolve_prod_selection("v1.2.3", repo)["ok"] is True

    local_result = dashboard._resolve_prod_selection("local:nullion/existing", repo)
    reset_local_result = dashboard._resolve_prod_selection("local:nullion/existing", repo, release_only=True)

    assert local_result["ok"] is True
    assert local_result["fetch"] == "app_checkout"
    assert local_result["source_mode"] == "local_app_checkout"
    assert reset_local_result["ok"] is False
    assert "reset prod only accepts release tags" in reset_local_result["error"]


def test_pin_branch_preserves_selection_and_existing_files(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    state_file = tmp_path / "pins.json"
    legacy_file = tmp_path / "existing.xlsx"
    legacy_file.write_bytes(b"preserve existing content")
    state_file.write_text(json.dumps({
        "app": {"selection": "local:nullion/existing", "bug_tracker": {"path": str(legacy_file)}},
        "qa": {"path": str(legacy_file)}, "other_setting": True,
    }))
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", state_file)
    before = dashboard.pinned_branch_status()
    assert "qa" not in before
    assert "bug_tracker" not in before["app"]
    for _ in range(2):
        result = dashboard.pin_branch("app", "local:nullion/existing")
        assert result["ok"] is True, result
        assert "bug_tracker" not in result
    saved = json.loads(state_file.read_text())
    assert saved["app"]["selection"] == "local:nullion/existing"
    assert "commit" not in saved["app"]
    assert "bug_tracker" not in saved["app"]
    assert "qa" not in saved
    assert saved["other_setting"] is True
    assert legacy_file.read_bytes() == b"preserve existing content"
    assert dashboard.pinned_branch_status()["app"]["exists"] is True


def test_create_and_pin_branch_uses_local_prefix(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    state_file = tmp_path / "pins.json"
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", state_file)
    result = dashboard.create_work_branch("generated-pin", "local:main")
    assert result["ok"] is True, result
    assert result["branch"] == "nullion/generated-pin"
    assert _git(repo, "show-ref", "--verify", "--quiet", "refs/heads/nullion/generated-pin") == ""
    saved = json.loads(state_file.read_text())
    assert set(saved) == {"app"}
    assert saved["app"]["selection"] == "local:nullion/generated-pin"


def test_create_and_repin_one_work_branch_removes_legacy_test_pin(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    state_file = tmp_path / "pins.json"
    state_file.write_text(json.dumps({"test": {"repo": str(tmp_path / "missing-old-repo"),
                                            "selection": "local:retired"}}))
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "PINNED_BRANCH_FILE", state_file)
    result = dashboard.create_work_branch("unified-fix", "local:main")
    assert result["ok"] is True, result
    assert result["branch"] == "nullion/unified-fix"
    repinned = dashboard.pin_work_branch("local:nullion/unified-fix")
    assert repinned["ok"] is True
    assert set(json.loads(state_file.read_text())) == {"app"}
    assert set(dashboard.pinned_branch_status()) == {"app", "state_file"}
    assert set(dashboard.action_summary("create_work_branch", result)["details"]) == {"branch"}
    dashboard.unpin_work_branch()
    assert json.loads(state_file.read_text()) == {}


def test_start_prod_local_branch_requires_explicit_allowance(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "DEV_WORKTREE_DIR", tmp_path / "dev-worktrees")

    result = dashboard.start_prod("local:nullion/existing")

    assert result["ok"] is False
    assert "requires checking" in result["error"]
    assert result["selection"] == "local:nullion/existing"


def test_start_prod_local_branch_uses_local_source_when_allowed(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    repo = tmp_path / "app"
    _init_repo(repo)
    monkeypatch.setattr(dashboard, "APP_REPO", repo)
    monkeypatch.setattr(dashboard, "DEV_WORKTREE_DIR", tmp_path / "dev-worktrees")
    captured: dict[str, object] = {}

    def fake_start(instance, source, *, job=None, skip_source_isolation=False):  # noqa: ANN001, ARG001
        captured["instance"] = instance.key
        captured["source"] = source
        captured["skip_source_isolation"] = skip_source_isolation
        return {"ok": True, "instance": instance.key, "source": str(source["path"])}

    monkeypatch.setattr(dashboard, "_start_instance", fake_start)

    result = dashboard.start_prod("local:nullion/existing", allow_local=True)

    assert result["ok"] is True, result
    assert captured["instance"] == "prod"
    assert captured["skip_source_isolation"] is True
    source = captured["source"]
    assert source["ok"] is True
    assert source["branch"] == "nullion/existing"
    assert Path(source["path"]).exists()
    assert source["path"] != Path(dashboard.PROD.expected_source)


def test_prod_instance_status_skips_status_api_probe(monkeypatch) -> None:
    dashboard = _load_dashboard()
    requested_urls: list[str] = []

    monkeypatch.setattr(dashboard, "_launchctl", lambda service: {"pid": None, "key": service.key})
    monkeypatch.setattr(dashboard, "_ps", lambda pid: {})
    monkeypatch.setattr(dashboard, "_pip_show", lambda instance: {"editable_source": ""})
    monkeypatch.setattr(dashboard, "_runtime_version", lambda instance: {"ok": True})
    monkeypatch.setattr(dashboard, "_http_probe", lambda url, **kwargs: {"ok": True, "status": 200, "elapsed_ms": 1, "bytes": 1})
    monkeypatch.setattr(dashboard, "_websocket_probe", lambda instance, **kwargs: {"ok": True, "elapsed_ms": 1})
    monkeypatch.setattr(dashboard, "_git_info", lambda path: {})
    monkeypatch.setattr(dashboard, "_port_listener", lambda port: {})
    monkeypatch.setattr(dashboard, "_db_stats", lambda path: {})
    monkeypatch.setattr(dashboard, "_env_summary", lambda instance: {})
    monkeypatch.setattr(dashboard, "_tail", lambda path, **kwargs: {})

    def fake_http_json(url: str, **kwargs):
        requested_urls.append(url)
        return {"ok": True, "status": 200, "elapsed_ms": 1, "payload": {}}

    monkeypatch.setattr(dashboard, "_http_json", fake_http_json)

    prod = dashboard.instance_status(dashboard.PROD)
    dev = dashboard.instance_status(dashboard.DEV)

    assert f"{dashboard.PROD.url}/api/status" not in requested_urls
    assert prod["status_endpoint"]["skipped"] is True
    assert f"{dashboard.DEV.url}/api/status" in requested_urls
    assert dev["status_endpoint"]["status"] == 200


def test_reset_prod_to_release_detaches_and_cleans_prod_source(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    source = tmp_path / "prod" / "src"
    source.parent.mkdir()
    _init_repo(source)
    _git(source, "tag", "v1.0.0")
    (source / "README.md").write_text("dirty\n", encoding="utf-8")
    (source / "scratch.py").write_text("remove me\n", encoding="utf-8")
    install_dir = tmp_path / "prod"
    python = install_dir / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/bin/sh\nprintf 'migration ok\\n'\n", encoding="utf-8")
    python.chmod(0o755)
    prod = dashboard.Instance(
        key="prod",
        title="Prod",
        url="http://127.0.0.1:8742",
        port=8742,
        install_dir=install_dir,
        expected_source=str(source),
        services=(),
    )
    monkeypatch.setattr(dashboard, "PROD", prod)
    monkeypatch.setattr(dashboard, "LAUNCH_AGENTS_DIR", tmp_path / "LaunchAgents")
    monkeypatch.setattr(dashboard, "_http_probe", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(dashboard, "open_webview", lambda instance: {"ok": True, "instance": instance.key})
    monkeypatch.setattr(
        dashboard,
        "_prod_release_install_verification",
        lambda: {"ok": True, "issues": [], "package": {"editable_source": ""}, "runtime_version": {}},
    )

    result = dashboard.reset_prod_to_release("v1.0.0")

    assert result["ok"] is True, result
    assert result["branch"] == "v1.0.0"
    assert "HEAD (no branch)" in result["status"]
    assert _git(source, "status", "--short") == ""
    assert not (source / "scratch.py").exists()
    assert result["source"] == str(source)
    assert result["source_mode"] == "release_tag"
    assert result["install"]["ok"] is True
    assert result["install"]["verification"]["ok"] is True
    assert result["post_install_clean"]["ok"] is True
    plists = list((tmp_path / "LaunchAgents").glob("*.plist"))
    assert all(str(source) not in plist.read_text(encoding="utf-8") for plist in plists)


def test_prod_release_install_verification_rejects_editable_or_local_sources(tmp_path, monkeypatch) -> None:
    dashboard = _load_dashboard()
    install_dir = tmp_path / "prod"
    prod = dashboard.Instance(
        key="prod",
        title="Prod",
        url="http://127.0.0.1:8742",
        port=8742,
        install_dir=install_dir,
        expected_source=str(install_dir / "src"),
        services=(),
    )
    monkeypatch.setattr(dashboard, "PROD", prod)
    monkeypatch.setattr(
        dashboard,
        "_pip_show",
        lambda instance: {
            "ok": True,
            "location": str(install_dir / "venv" / "lib" / "python3.11" / "site-packages"),
            "editable_source": "",
        },
    )
    monkeypatch.setattr(
        dashboard,
        "_runtime_version",
        lambda instance: {
            "ok": True,
            "module": str(install_dir / "venv" / "lib" / "python3.11" / "site-packages" / "nullion" / "__init__.py"),
        },
    )

    assert dashboard._prod_release_install_verification()["ok"] is True
    assert dashboard._instance_source_matches_expected(prod, dashboard._pip_show(prod)) is True

    monkeypatch.setattr(
        dashboard,
        "_pip_show",
        lambda instance: {
            "ok": True,
            "location": str(dashboard.APP_REPO / "src"),
            "editable_source": str(dashboard.APP_REPO),
        },
    )
    bad = dashboard._prod_release_install_verification()

    assert bad["ok"] is False
    assert any("editable_source still set" in issue for issue in bad["issues"])
    assert any("local/dev source reference remains" in issue for issue in bad["issues"])


def test_stop_instance_waits_for_services_and_escalates_stale_port(monkeypatch, tmp_path) -> None:
    dashboard = _load_dashboard()
    service = dashboard.Service("web", "com.nullion.test.web", "out.log", "err.log")
    instance = dashboard.Instance(
        key="dev",
        title="Dev",
        url="http://127.0.0.1:8752",
        port=8752,
        install_dir=tmp_path,
        expected_source=str(tmp_path),
        services=(service,),
    )
    port_checks = iter([["111"], ["111"]])
    port_waits = iter([{"ok": False, "pids": ["111"]}, {"ok": True, "pids": []}])
    kills: list[str] = []

    monkeypatch.setattr(dashboard, "_bootout_service", lambda service, job=None: {"ok": True, "label": service.label})
    monkeypatch.setattr(dashboard, "_launchctl", lambda service: {"loaded": False, "state": "missing", "pid": None})
    monkeypatch.setattr(dashboard, "_port_listener_pids", lambda instance: next(port_checks))
    monkeypatch.setattr(dashboard, "_wait_for_port_free", lambda instance, timeout=10.0: next(port_waits))

    def fake_run(args, *, timeout=4):  # noqa: ANN001, ARG001
        if args[:2] == ["ps", "-axo"]:
            return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
        kills.append(args[1])
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.setattr(dashboard, "_run", fake_run)

    result = dashboard.stop_instance(instance)

    assert result["ok"] is True, result
    assert kills == ["-TERM", "-KILL"]
    assert result["port_free"]["ok"] is True


def test_bootstrap_service_retries_unloaded_launchagent_and_kills_stale_tray(monkeypatch, tmp_path) -> None:
    dashboard = _load_dashboard()
    service = dashboard.Service("tray", "com.nullion.tray", "out.log", "err.log")
    install_dir = tmp_path / "install"
    (install_dir / "venv" / "bin").mkdir(parents=True)
    instance = dashboard.Instance(
        key="prod",
        title="Prod",
        url="http://127.0.0.1:8742",
        port=8742,
        install_dir=install_dir,
        expected_source=str(install_dir / "src"),
        services=(service,),
    )
    plist = tmp_path / "com.nullion.tray.plist"
    plist.write_text("<plist/>", encoding="utf-8")
    monkeypatch.setattr(dashboard, "_plist_for", lambda instance, service: plist)

    bootstrap_attempts = 0
    loaded = False
    stale = True
    killed: list[list[str]] = []

    def fake_launchctl(service):  # noqa: ANN001
        if loaded:
            return {"loaded": True, "state": "running", "pid": "555"}
        return {"loaded": False, "state": "missing", "pid": None}

    def fake_run(args, *, timeout=4):  # noqa: ANN001, ARG001
        nonlocal stale
        if args[:2] == ["ps", "-axo"]:
            stdout = ""
            if stale:
                stdout = (
                    f"111 {install_dir}/venv/bin/nullion-tray --port 8742\n"
                    f"112 {install_dir}/venv/bin/python -m nullion.webview_app --port 8742\n"
                )
            return {"ok": True, "stdout": stdout, "stderr": "", "returncode": 0}
        if args[:1] == ["kill"]:
            killed.append(args)
            stale = False
            return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
        raise AssertionError(f"unexpected command: {args}")

    def fake_run_step(job, message, command, timeout=0):  # noqa: ANN001, ARG001
        nonlocal bootstrap_attempts, loaded
        if command[:2] == ["launchctl", "bootstrap"]:
            bootstrap_attempts += 1
            if bootstrap_attempts == 1:
                return {"ok": False, "stdout": "", "stderr": "not ready", "returncode": 5}
            loaded = True
            return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
        if command[:3] == ["launchctl", "kickstart", "-k"]:
            return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(dashboard, "_launchctl", fake_launchctl)
    monkeypatch.setattr(dashboard, "_run", fake_run)
    monkeypatch.setattr(dashboard, "_run_step", fake_run_step)

    result = dashboard._bootstrap_service(instance, service)

    assert result["ok"] is True, result
    assert bootstrap_attempts == 2
    assert ["kill", "-TERM", "111"] in killed
    assert ["kill", "-TERM", "112"] in killed


def test_start_instance_requires_stop_and_opens_webview(monkeypatch, tmp_path) -> None:
    dashboard = _load_dashboard()
    source = tmp_path / "source"
    expected = source / "src" / "nullion" / "__init__.py"
    expected.parent.mkdir(parents=True)
    expected.write_text("", encoding="utf-8")
    install_dir = tmp_path / "install"
    python = install_dir / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/bin/sh\n", encoding="utf-8")
    service = dashboard.Service("web", "com.nullion.test.web", "out.log", "err.log")
    instance = dashboard.Instance(
        key="dev",
        title="Dev",
        url="http://127.0.0.1:8752",
        port=8752,
        install_dir=install_dir,
        expected_source=str(source),
        services=(service,),
    )
    opened: list[str] = []

    monkeypatch.setattr(dashboard, "_ensure_instance_venv", lambda instance: {"ok": True})
    monkeypatch.setattr(dashboard, "_write_instance_plists", lambda instance: None)
    monkeypatch.setattr(dashboard, "stop_instance", lambda instance, job=None: {"ok": True})
    monkeypatch.setattr(dashboard, "_bootstrap_service", lambda instance, service, job=None: {"ok": True})
    monkeypatch.setattr(dashboard, "_http_json", lambda url, **kwargs: {"ok": True})
    monkeypatch.setattr(dashboard, "open_webview", lambda instance: opened.append(instance.key) or {"ok": True})

    def fake_run_step(job, message, command, timeout=0):  # noqa: ANN001, ARG001
        if message == "Verify import source":
            return {"ok": True, "stdout": str(expected), "stderr": "", "returncode": 0}
        return {"ok": True, "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.setattr(dashboard, "_run_step", fake_run_step)

    result = dashboard._start_instance(
        instance,
        {"ok": True, "path": source, "branch": "nullion/example", "commit": "abc", "source_mode": "local_worktree"},
    )

    assert result["ok"] is True, result
    assert opened == ["dev"]
    assert result["webview"]["ok"] is True


def test_start_instance_fails_when_existing_runtime_does_not_stop(monkeypatch, tmp_path) -> None:
    dashboard = _load_dashboard()
    source = tmp_path / "source"
    install_dir = tmp_path / "install"
    python = install_dir / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/bin/sh\n", encoding="utf-8")
    instance = dashboard.Instance(
        key="stage",
        title="Stage",
        url="http://127.0.0.1:8753",
        port=8753,
        install_dir=install_dir,
        expected_source=str(source),
        services=(),
    )
    install_calls: list[str] = []

    monkeypatch.setattr(dashboard, "_ensure_instance_venv", lambda instance: {"ok": True})
    monkeypatch.setattr(dashboard, "_write_instance_plists", lambda instance: None)
    monkeypatch.setattr(dashboard, "stop_instance", lambda instance, job=None: {"ok": False, "port_free": {"ok": False}})
    monkeypatch.setattr(dashboard, "_run_step", lambda *args, **kwargs: install_calls.append("run") or {"ok": True})

    result = dashboard._start_instance(
        instance,
        {"ok": True, "path": source, "branch": "branch"},
        skip_source_isolation=True,
    )

    assert result["ok"] is False
    assert "did not fully stop" in result["error"]
    assert install_calls == []

from __future__ import annotations

import base64
import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion import crons, providers, updater, webview_app
from nullion.tools import (
    ToolInvocation,
    _build_create_cron_handler,
    _build_delete_cron_handler,
    _build_list_crons_handler,
    _build_run_cron_handler,
    _build_toggle_cron_handler,
)


def invoke(tool_name: str, arguments: dict[str, object]) -> ToolInvocation:
    return ToolInvocation(f"inv-{tool_name}", tool_name, "user:1", arguments)


def test_cron_tool_handlers_cover_create_list_delete_toggle_and_run(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(crons, "_CRONS_PATH", tmp_path / "crons.json")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")
    monkeypatch.setattr("nullion.artifacts.is_safe_artifact_path", lambda path: True)

    create = _build_create_cron_handler(default_delivery_channel="web", default_delivery_target="web:operator")
    assert create(invoke("create_cron", {})).error == "name, schedule and task are all required"
    created = create(
        invoke(
            "create_cron",
            {
                "name": "Morning Brief",
                "schedule": "*/5 * * * *",
                "task": "Summarize",
                "workspace_id": "workspace-a",
            },
        )
    )
    assert created.status == "completed"
    cron_id = created.output["id"]

    listed = _build_list_crons_handler()(invoke("list_crons", {}))
    assert listed.output["crons"][0]["name"] == "Morning Brief"

    toggle = _build_toggle_cron_handler()
    assert toggle(invoke("toggle_cron", {})).error == "id is required"
    assert toggle(invoke("toggle_cron", {"id": "missing"})).error == "No cron found with id='missing'"
    disabled = toggle(invoke("toggle_cron", {"id": cron_id, "enabled": False}))
    assert disabled.output["enabled"] is False

    artifact = tmp_path / "artifact.txt"
    artifact.write_text("done", encoding="utf-8")
    run = _build_run_cron_handler(lambda job: {"text": f"finished\nMEDIA:{artifact}", "artifact_paths": [{"path": str(artifact)}]})
    assert run(invoke("run_cron", {})).error == "id or name is required"
    by_name = run(invoke("run_cron", {"name": "morning brief"}))
    assert by_name.status == "completed"
    assert by_name.output["artifact_paths"] == [str(artifact.resolve(strict=False))]

    failed_run = _build_run_cron_handler(lambda job: {"reached_iteration_limit": True, "text": "stopped"})
    assert failed_run(invoke("run_cron", {"id": cron_id})).status == "failed"
    assert _build_run_cron_handler(None)(invoke("run_cron", {"id": cron_id})).error == "This runtime can list crons but cannot run them on demand."

    second = create(invoke("create_cron", {"name": "Morning Brief Copy", "schedule": "*/10 * * * *", "task": "Summarize", "workspace_id": "workspace-a"}))
    ambiguous = run(invoke("run_cron", {"name": "morning"}))
    assert ambiguous.status == "failed"
    assert "Multiple crons matched" in ambiguous.error

    delete = _build_delete_cron_handler()
    assert delete(invoke("delete_cron", {})).error == "id is required"
    assert delete(invoke("delete_cron", {"id": second.output["id"]})).status == "completed"
    assert delete(invoke("delete_cron", {"id": "missing"})).error == "No cron found with id='missing'"


def test_provider_helpers_search_media_and_custom_api(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MEDIA_OPENAI_API_KEY", "media-key")
    assert providers._provider_key_for_media("openai") == "media-key"
    monkeypatch.delenv("NULLION_MEDIA_OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "provider-key")
    assert providers._provider_key_for_media("openai") == "provider-key"
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_ENABLED", "off")
    assert providers._media_model_selection("NULLION_AUDIO_TRANSCRIBE_PROVIDER", "NULLION_AUDIO_TRANSCRIBE_MODEL", "NULLION_AUDIO_TRANSCRIBE_ENABLED") is None
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_ENABLED", "true")
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_PROVIDER", "openai")
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_MODEL", "whisper")
    assert providers._media_model_selection("NULLION_AUDIO_TRANSCRIBE_PROVIDER", "NULLION_AUDIO_TRANSCRIBE_MODEL", "NULLION_AUDIO_TRANSCRIBE_ENABLED") == ("openai", "whisper")

    monkeypatch.setattr(providers.shutil, "which", lambda name: f"/usr/bin/{name}" if name == "tool" else None)
    assert providers._which_local_tool("tool") == "tool"
    assert providers._first_env("MISSING", "OPENAI_API_KEY") == "provider-key"
    with pytest.raises(RuntimeError):
        providers._require_env("MISSING", provider_name="demo")
    assert providers._clamped_limit(99, max_limit=3) == 3
    assert providers._result(" Title ", " https://example.com ", " Snip ") == {"title": "Title", "url": "https://example.com", "snippet": "Snip"}
    assert providers._result("", "url") is None

    payloads: list[tuple[str, dict | None]] = []

    def fake_request_json(url, **kwargs):
        payloads.append((url, kwargs.get("payload")))
        if "brave" in url:
            return {"web": {"results": [{"title": "One", "url": "https://one", "description": "desc"}, "bad"]}}
        if "customsearch" in url:
            return {"items": [{"title": "Two", "link": "https://two", "snippet": "desc"}]}
        if "perplexity" in url:
            return {"results": [{"title": "Three", "url": "https://three", "snippet": "desc", "date": "2026-01-01"}]}
        if "duckduckgo" in url:
            return {
                "Heading": "Duck",
                "AbstractURL": "https://duck",
                "AbstractText": "summary",
                "RelatedTopics": [{"Topics": [{"Text": "Nested", "FirstURL": "https://nested"}]}],
            }
        return {"results": [{"id": "m1"}], "message": {"id": "m1"}}

    monkeypatch.setattr(providers, "_request_json", fake_request_json)
    monkeypatch.setenv("NULLION_BRAVE_SEARCH_API_KEY", "brave")
    monkeypatch.setenv("NULLION_GOOGLE_SEARCH_API_KEY", "google")
    monkeypatch.setenv("NULLION_GOOGLE_SEARCH_CX", "cx")
    monkeypatch.setenv("NULLION_PERPLEXITY_API_KEY", "perp")
    assert providers._brave_web_search("q", 2)[0]["title"] == "One"
    assert providers._google_custom_search("q", 2)[0]["title"] == "Two"
    assert providers._perplexity_search("q", 2)[0]["date"] == "2026-01-01"
    assert [r["title"] for r in providers._duckduckgo_instant_answer_search("q", 3)] == ["Duck", "Nested"]

    monkeypatch.setattr(providers.subprocess, "run", lambda argv, **kwargs: SimpleNamespace(returncode=0, stdout="generated text\n", stderr=""))
    monkeypatch.setattr(providers.shutil, "which", lambda name: name)
    assert providers._run_media_command_template("echo {input}", substitutions={"input": "hello"}).stdout == "generated text\n"
    with pytest.raises(RuntimeError, match="missing placeholder"):
        providers._run_media_command_template("echo {missing}", substitutions={})
    with pytest.raises(RuntimeError, match="empty"):
        providers._run_media_command_template("   ", substitutions={})
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_COMMAND", "echo {input}")
    assert providers._local_audio_transcribe(str(tmp_path / "audio.wav"), "en")["text"] == "generated text"
    monkeypatch.setenv("NULLION_IMAGE_OCR_COMMAND", "echo {input}")
    assert providers._local_image_extract_text(str(tmp_path / "image.png"))["text"] == "generated text"

    out = tmp_path / "image.png"
    monkeypatch.setattr(providers, "messaging_media_scratch_root", lambda: tmp_path)
    monkeypatch.setattr(providers, "_run_media_command_template", lambda *args, **kwargs: out.write_bytes(b"png") or SimpleNamespace(stdout=""))
    assert providers._command_image_generate("make {output}", "draw", str(out), "1024x1024")["path"] == str(out)
    providers._write_image_url_to_path("data:image/png;base64," + base64.b64encode(b"img").decode(), tmp_path / "data.png")
    assert (tmp_path / "data.png").read_bytes() == b"img"
    assert providers._openrouter_image_config("1200x800") == {"aspect_ratio": "3:2"}
    assert providers._gemini_model_name("models/gemini") == "gemini"

    monkeypatch.setenv("NULLION_CUSTOM_API_TOKEN", "token")
    assert providers._custom_api_base_url(SimpleNamespace(provider_profile="https://api.example.com/root")) == "https://api.example.com/root"
    assert providers._custom_api_headers(None)["Authorization"] == "Bearer token"
    monkeypatch.setattr(providers, "_custom_api_connection", lambda principal_id: SimpleNamespace(provider_profile="https://api.example.com", credential_ref="NULLION_CUSTOM_API_TOKEN"))
    assert providers._custom_api_email_search("hi", 2)[0]["id"] == "m1"
    assert providers._custom_api_email_read("m1")["id"] == "m1"

    assert "web_searcher" in providers.resolve_plugin_provider_kwargs(plugin_name="search_plugin", provider_name="duckduckgo_search_provider")
    assert "image_generator" in providers.resolve_plugin_provider_kwargs(plugin_name="media_plugin", provider_name="local_media_provider")
    with pytest.raises(ValueError):
        providers.resolve_plugin_provider_kwargs(plugin_name="missing", provider_name="x")


def test_updater_helpers_rollback_and_health_checks(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    src = tmp_path / "src"
    src.mkdir()
    (src / ".git").mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    monkeypatch.setenv("NULLION_SRC_DIR", str(src))
    assert updater._install_dir() == install
    assert updater._src_dir() == src
    assert updater._backup_dir().exists()
    assert updater._version_label("bad", cwd=src) == "bad"

    git_calls: list[tuple[str, ...]] = []

    def fake_git(*args, cwd=None):
        git_calls.append(args)
        if args[:2] == ("rev-parse", "--verify"):
            return 0, "a" * 40
        if args[:2] == ("reset", "--hard"):
            return 0, "reset"
        if args[0] == "merge-base":
            return 1, ""
        if args[0] == "describe":
            return 1, ""
        if args[0] == "rev-parse":
            return 0, "abc1234"
        return 0, ""

    monkeypatch.setattr(updater, "_git", fake_git)
    assert updater._commit_relation("a", "a") == "current"
    assert updater._commit_relation("a", "b") == "diverged"
    assert updater._validate_commit_hash("abc1234") == "a" * 40
    with pytest.raises(ValueError):
        updater._validate_commit_hash("not a hash")
    updater._validate_requirements_content("requests==1\n--index-url https://example.com\n")
    with pytest.raises(ValueError):
        updater._validate_requirements_content("; rm -rf /\n")

    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "git_commit.txt").write_text("abc1234", encoding="utf-8")
    (snap / "requirements.txt").write_text("requests==1\n", encoding="utf-8")
    monkeypatch.setattr(updater, "_venv_pip", lambda: Path("pip"))
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout='{"status":"ok"}', stderr=""))
    assert updater.rollback(snap) is True

    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr="bad"))
    assert updater._probe_messaging_platform_bootstrap(env_path=None, checkpoint_path=tmp_path / "runtime.json") is False
    warnings: list[str] = []
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="PROVIDER_WARNING: missing\nok\n", stderr=""))
    monkeypatch.setattr(updater, "_probe_web_status", lambda **kwargs: True)
    monkeypatch.setattr(updater, "_probe_messaging_platform_bootstrap", lambda **kwargs: True)
    assert updater.health_check(_warnings_out=warnings) is True
    assert warnings == ["missing"]
    monkeypatch.setattr(updater, "_probe_web_status", lambda **kwargs: False)
    assert updater.health_check(require_messaging=False) is False

    fresh_warnings: list[str] = []
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="PROVIDER_WARNING: fresh\n", stderr=""))
    assert updater.fresh_health_check(_warnings_out=fresh_warnings) is True
    assert fresh_warnings == ["fresh"]
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="no", stderr=""))
    assert updater.fresh_health_check() is False


def test_run_update_workflow_handles_no_update_success_and_rollback(tmp_path, monkeypatch) -> None:
    emitted: list[updater.UpdateProgress] = []
    target = updater.UpdateTarget(channel="release", ref="v2", commit="new1234", label="v2")

    monkeypatch.setattr(updater, "_current_commit", lambda: "old1234")
    monkeypatch.setattr(updater, "_update_target", lambda channel: target)
    monkeypatch.setattr(updater, "_version_label", lambda ref="HEAD": "v1" if ref == "HEAD" else ref)
    monkeypatch.setattr(updater, "_commit_relation", lambda local, remote: "current")

    no_update = asyncio.run(updater.run_update(emit=emitted.append))

    assert no_update.success is True
    assert no_update.from_version == "v1"
    assert no_update.to_version == "v1"
    assert [step.step for step in emitted] == ["fetch", "fetch"]

    emitted.clear()
    snapshot_path = tmp_path / "snapshot"
    snapshot_path.mkdir()
    git_calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(updater, "_commit_relation", lambda local, remote: "behind")
    monkeypatch.setattr(updater, "_probe_web_status", lambda **kwargs: True)
    monkeypatch.setattr(updater, "snapshot", lambda: snapshot_path)
    monkeypatch.setattr(updater, "_git", lambda *args, **kwargs: git_calls.append(args) or (0, "ok"))
    monkeypatch.setattr(updater, "_venv_pip", lambda: tmp_path / "pip")
    monkeypatch.setattr(updater, "_src_dir", lambda: tmp_path)
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""))
    monkeypatch.setattr(updater, "fresh_health_check", lambda **kwargs: True)

    updated = asyncio.run(updater.run_update(emit=emitted.append))

    assert updated.success is True
    assert updated.from_version == "v1"
    assert updated.to_version == "v2"
    assert updated.snapshot_path == str(snapshot_path)
    assert ("reset", "--hard", "v2") in git_calls
    assert ("clean", "-fd") in git_calls
    assert emitted[-1].step == "done"

    emitted.clear()
    rolled_back: list[Path | None] = []
    monkeypatch.setattr(updater, "fresh_health_check", lambda **kwargs: False)
    monkeypatch.setattr(updater, "rollback", lambda snap: rolled_back.append(snap) or True)

    failed = asyncio.run(updater.run_update(emit=emitted.append))

    assert failed.success is False
    assert failed.rolled_back is True
    assert failed.error == "Health check failed after update."
    assert rolled_back == [snapshot_path]
    assert [step.step for step in emitted][-2:] == ["health", "rollback"]


def test_webview_helpers_single_instance_sizing_and_cli(tmp_path, monkeypatch) -> None:
    pid_file = tmp_path / "webview.pid"
    monkeypatch.setattr(webview_app, "_WEBVIEW_PID_FILE", pid_file)
    monkeypatch.setattr(webview_app, "_NULLION_HOME", tmp_path)
    assert webview_app._pid_is_running(-1) is False
    monkeypatch.setattr(webview_app.os, "kill", lambda pid, sig: None)
    assert webview_app._pid_is_running(123) is True
    monkeypatch.setattr(webview_app, "_focus_process", lambda pid: True)
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text("123", encoding="utf-8")
    assert webview_app._claim_single_instance() is False
    monkeypatch.setattr(webview_app, "_focus_process", lambda pid: False)
    assert webview_app._claim_single_instance() is True

    webview = SimpleNamespace(screens=[SimpleNamespace(width=3000, height=1600)])
    assert webview_app._resolve_window_size(webview) == (1920, 1200)
    monkeypatch.setenv("NULLION_WEBVIEW_WIDTH", "1000")
    monkeypatch.setenv("NULLION_WEBVIEW_HEIGHT", "700")
    assert webview_app._resolve_window_size(SimpleNamespace(screens=[])) == (1000, 700)
    monkeypatch.setenv("NULLION_WEBVIEW_WIDTH", "bad")
    assert webview_app._resolve_window_size(SimpleNamespace(screens=[])) == (1440, 900)

    icon_path = tmp_path / "icon.png"
    monkeypatch.setattr(webview_app, "_claim_single_instance", lambda: True)
    monkeypatch.setattr(webview_app, "_webview_icon_path", lambda: str(icon_path))
    monkeypatch.setattr(webview_app, "_set_macos_app_icon", lambda path: None)
    events: list[tuple] = []
    fake_webview = SimpleNamespace(
        screens=[SimpleNamespace(width=1400, height=900)],
        create_window=lambda *args, **kwargs: events.append(("window", args, kwargs)),
        start=lambda **kwargs: events.append(("start", kwargs)),
    )
    webview_app._run_webview(SimpleNamespace(width=800, height=600, debug=True), fake_webview, "http://localhost")
    assert events[0][2]["width"] == 800
    assert events[-1][1]["debug"] is True

    opened: list[str] = []
    monkeypatch.setattr(webview_app, "_load_env", lambda env_file: None)
    monkeypatch.setattr(webview_app.webbrowser, "open", lambda url: opened.append(url))
    monkeypatch.setattr(sys, "argv", ["nullion-webview", "--browser-fallback", "--path", "approvals"])
    monkeypatch.setitem(sys.modules, "webview", None)
    webview_app._cli_impl()
    assert opened[-1].endswith("/approvals")

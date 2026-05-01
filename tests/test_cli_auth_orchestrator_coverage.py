from __future__ import annotations

import base64
import sys
from types import SimpleNamespace

import pytest

from nullion import agent_orchestrator, auth, cli, updater
from nullion.credential_store import load_encrypted_credentials
from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


def test_agent_orchestrator_helper_paths(tmp_path, monkeypatch) -> None:
    completed = ToolResult("i", "file_write", "completed", {"path": str(tmp_path / "out.txt")})
    assert agent_orchestrator._artifact_paths_from_tool_result(completed) == [str(tmp_path / "out.txt")]
    generated = ToolResult("i", "image_generate", "completed", {"path": "a.png", "output_path": "a.png"})
    assert agent_orchestrator._artifact_paths_from_tool_result(generated) == ["a.png"]
    assert agent_orchestrator._artifact_paths_from_tool_result(ToolResult("i", "x", "failed", {})) == []

    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    screenshot_path = artifact_root / "shot.png"
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_runtime", lambda runtime_store: artifact_root)
    monkeypatch.setattr("nullion.artifacts.artifact_descriptor_for_path", lambda path, artifact_root: {"path": str(path)})
    monkeypatch.setattr("nullion.artifacts.artifact_path_for_generated_file", lambda runtime_store, suffix: screenshot_path)
    runtime_store = object()
    before = agent_orchestrator._artifact_root_snapshot(runtime_store)
    (artifact_root / "new.txt").write_text("new", encoding="utf-8")
    assert str((artifact_root / "new.txt").resolve()) in agent_orchestrator._new_artifact_paths_since(before, runtime_store=runtime_store)
    shot = ToolResult("i", "browser_screenshot", "completed", {"image_base64": base64.b64encode(b"png").decode()})
    assert agent_orchestrator._artifact_paths_from_tool_result(shot, runtime_store=runtime_store) == [str(screenshot_path)]
    assert shot.output["path"] == str(screenshot_path)

    untrusted = ToolResult("i", "web_fetch", "completed", {"url": "https://example.com", "text": "raw"})
    assert "untrusted" in agent_orchestrator._tool_result_message_payload(untrusted)
    assert agent_orchestrator._malformed_tool_call_result(principal_id="p", reason="bad", block={"name": "tool"}).tool_name == "tool"
    assert "last tool result" in agent_orchestrator._last_useful_tool_message([ToolResult("i", "tool", "completed", {"message": "hello"})])
    assert "failed" in agent_orchestrator._last_useful_tool_message([ToolResult("i", "tool", "failed", {}, error="boom")])
    assert agent_orchestrator._is_bare_completion_text("Done!") is True
    assert agent_orchestrator._bare_completion_without_work_text("completed") == "I don't have a concrete result to report."
    assert agent_orchestrator._post_tool_delivery_nudge().startswith("You just executed")
    assert agent_orchestrator._conversation_visible_content([{"type": "thinking"}, {"type": "text", "text": "hi"}]) == [{"type": "text", "text": "hi"}]

    monkeypatch.setenv("NULLION_TOOL_LOOP_DOCTOR_THRESHOLD", "bad")
    monkeypatch.setenv("NULLION_REPEATED_TOOL_FAILURE_LIMIT", "0")
    assert agent_orchestrator._tool_loop_doctor_threshold() == 20
    assert agent_orchestrator._repeated_tool_failure_limit() == 1
    sig = agent_orchestrator._tool_invocation_signature(tool_name="tool", tool_input={"b": 2})
    failed = ToolResult("i", "tool", "denied", {"reason": "policy"}, error="no")
    fingerprint = agent_orchestrator._tool_failure_fingerprint(result=failed, invocation_signature=sig)
    assert fingerprint and "policy" in fingerprint
    assert agent_orchestrator._tool_failure_fingerprint(result=ToolResult("i", "tool", "completed", {}), invocation_signature=sig) is None
    assert "same non-completing" in agent_orchestrator._repeated_tool_failure_message(result=failed, repeated_count=3)

    delivered: list[tuple] = []
    agent_orchestrator._notify_long_running_tool_loop(lambda *args, **kwargs: delivered.append((args, kwargs)), conversation_id="c", tool_results=[failed])
    assert delivered[0][1]["kind"] == "doctor_progress"
    registry = ToolRegistry()
    registry.register_cleanup_hook(lambda scope_id: (_ for _ in ()).throw(RuntimeError("cleanup failed")))
    agent_orchestrator._run_tool_cleanup_hooks(registry, "scope")


def test_cli_update_skill_pack_display_and_model_helpers(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(cli, "_open_update_entrypoint", lambda **kwargs: print(f"open:{kwargs['version_changed']}"))

    async def no_update(**kwargs):
        return SimpleNamespace(success=True, from_version="v1", to_version="v1", snapshot_path="", rolled_back=False, error="")

    monkeypatch.setattr(updater, "run_update", no_update)
    with pytest.raises(SystemExit) as exc:
        cli._run_update_cli()
    assert exc.value.code == 0
    assert "Already up to date" in capsys.readouterr().out

    async def updated(**kwargs):
        return SimpleNamespace(success=True, from_version="v1", to_version="v2", snapshot_path="", rolled_back=False, error="")

    monkeypatch.setattr(updater, "run_update", updated)
    monkeypatch.setattr("builtins.input", lambda prompt="": "n")
    with pytest.raises(SystemExit) as exc:
        cli._run_update_cli()
    assert exc.value.code == 0
    assert "when ready" in capsys.readouterr().out

    async def failed(**kwargs):
        return SimpleNamespace(success=False, from_version="v1", to_version="v2", snapshot_path="", rolled_back=True, error="boom")

    monkeypatch.setattr(updater, "run_update", failed)
    with pytest.raises(SystemExit) as exc:
        cli._run_update_cli()
    assert exc.value.code == 1
    assert "Update failed" in capsys.readouterr().out

    env_path = tmp_path / ".env"
    env_path.write_text("# keep\nNULLION_MODEL=\"old\"\nOTHER=x\n", encoding="utf-8")
    cli._merge_env_updates(env_path, {"NULLION_MODEL": "new", "NULLION_MODEL_PROVIDER": "openai"})
    assert 'NULLION_MODEL="new"' in env_path.read_text(encoding="utf-8")
    assert 'NULLION_MODEL_PROVIDER="openai"' in env_path.read_text(encoding="utf-8")
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setitem(sys.modules, "nullion.skill_pack_installer", SimpleNamespace(
        BUILTIN_SKILL_PACK_PROMPTS={"builtin/pack": "prompt"},
        get_installed_skill_pack=lambda pack_id: None,
        normalize_pack_id=lambda pack_id: pack_id.strip().lower(),
        list_installed_skill_packs=lambda: [SimpleNamespace(pack_id="p1", skills_count=2, warnings=["w"], source="src", path=tmp_path)],
        install_skill_pack=lambda source, pack_id=None, force=False: SimpleNamespace(pack_id=pack_id or "p2", source=source, skills_count=1, warnings=["warn"]),
    ))
    assert cli._enable_skill_pack_id("BUILTIN/PACK") == "builtin/pack"
    cli._run_skill_pack_cli(SimpleNamespace(skill_pack_command="list"))
    assert "p1" in capsys.readouterr().out
    cli._run_skill_pack_cli(SimpleNamespace(skill_pack_command="install", source="repo", pack_id="new/pack", force=False, no_enable=True))
    assert "Installed new/pack" in capsys.readouterr().out

    store = SimpleNamespace(
        task_frames={"f": SimpleNamespace(status="active", summary="doing", target=SimpleNamespace(value="target"))},
        list_approval_requests=lambda: [SimpleNamespace(status="pending", approval_id="approval-123456", tool_name="tool")],
        list_reminders=lambda: [SimpleNamespace(delivered_at=None)],
        skills={"s": SimpleNamespace(title="Skill", trigger="when")},
    )
    cli._print_status(SimpleNamespace(store=store))
    cli._print_skills(SimpleNamespace(store=store))
    registry = SimpleNamespace(list_specs=lambda: [ToolSpec("z", "desc" * 30, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 1)])
    cli._print_tools(registry)
    output = capsys.readouterr().out
    assert "Task frames" in output and "Skill" in output and "tools registered" in output

    cli._switch_model(model="gpt-test", provider="openai")
    assert load_encrypted_credentials(db_path=tmp_path / ".nullion" / "runtime.db")["model"] == "gpt-test"
    assert cli._xml_escape('<a&"') == "&lt;a&amp;&quot;"
    monkeypatch.setenv("NULLION_WEB_PORT", "bad")
    assert cli._default_web_port() == 8742


def test_auth_api_key_save_reauth_and_cli_token_helpers(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(auth, "CREDENTIALS_PATH", tmp_path / "credentials.json")
    prompts = iter(["http://localhost:11434/v1", "llama", "n"])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(prompts))
    custom = auth._collect_api_key({"id": "custom"})
    assert custom["api_key"] == "none"
    assert custom["model"] == "llama"

    prompts = iter(["n", "model"])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(prompts))
    ollama = auth._collect_api_key({"id": "ollama", "optional_key": True, "default_model": "llama3", "key_hint": "hint"})
    assert ollama["api_key"] == "ollama-local"
    prompts = iter(["gpt-test"])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(prompts))
    monkeypatch.setattr(auth.getpass, "getpass", lambda prompt="": "sk-test")
    saved_path = auth.save_credentials_for_provider(
        {"id": "openai", "kind": "api_key", "default_model": "gpt-5.5", "key_hint": "sk-..."}
    )
    assert saved_path == auth.CREDENTIALS_PATH.with_name("runtime.db")
    assert auth.load_stored_credentials()["provider"] == "openai"

    monkeypatch.setattr(auth, "_codex_device_code_oauth", lambda: ("access", "refresh"))
    creds = auth.codex_oauth_credentials()
    assert creds["provider"] == "codex"
    auth._save({"provider": "openai", "model": "old", "keys": {"openai": "sk"}})
    assert auth.load_stored_credentials()["provider"] == "openai"
    path = auth.reauthenticate_codex_oauth()
    saved = auth.load_stored_credentials()
    assert path == auth.CREDENTIALS_PATH.with_name("runtime.db")
    assert saved["keys"]["codex"] == "access"
    assert saved["models"]["codex"] == "old"

    token_path = tmp_path / "token.txt"
    auth._cli_impl(["--write-codex-access-token", str(token_path)])
    assert token_path.read_text(encoding="utf-8") == "access"
    auth._cli_impl(["--print-codex-access-token"])
    assert "access" in capsys.readouterr().out

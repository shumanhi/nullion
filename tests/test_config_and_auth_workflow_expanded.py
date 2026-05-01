from __future__ import annotations

import json
import stat

import pytest


def test_env_file_parsing_permissions_and_override(tmp_path, monkeypatch) -> None:
    from nullion import config

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "export NULLION_TELEGRAM_OPERATOR_CHAT_ID='123'",
                'NULLION_MODEL="gpt-test"',
                "EMPTY=",
                "bad line",
            ]
        ),
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    values = config.load_env_file_into_environ(env_file, override=True)
    assert values["NULLION_TELEGRAM_OPERATOR_CHAT_ID"] == "123"
    assert values["NULLION_MODEL"] == "gpt-test"
    assert values["EMPTY"] == ""
    assert config.load_default_env_file_into_environ(env_file) == env_file

    monkeypatch.setenv("NULLION_MODEL", "existing")
    config.load_env_file_into_environ(env_file, override=False)
    assert config.os.environ["NULLION_MODEL"] == "existing"

    insecure = tmp_path / "insecure.env"
    insecure.write_text("A=B", encoding="utf-8")
    insecure.chmod(0o644)
    if config.os.name != "nt":
        with pytest.raises(RuntimeError, match="Insecure permissions"):
            config._read_env_file(insecure)


def test_settings_parse_terminal_plugins_slack_discord_and_stored_credentials(tmp_path, monkeypatch) -> None:
    from nullion import config

    credentials = {
        "provider": "codex",
        "api_key": "stored-access",
        "refresh_token": "stored-refresh",
        "base_url": "https://stored.example/v1",
        "model": "stored-model,backup",
        "reasoning_effort": "high",
        "keys": {"codex": "stored-key"},
        "models": {"codex": "stored-codex-model"},
    }
    monkeypatch.setattr("nullion.auth.load_stored_credentials", lambda: credentials)
    settings = config.load_settings(
        env_path=None,
        env={
            "NULLION_TELEGRAM_CHAT_ENABLED": "false",
            "NULLION_SLACK_ENABLED": "true",
            "NULLION_SLACK_BOT_TOKEN": "slack-bot",
            "NULLION_SLACK_APP_TOKEN": "slack-app",
            "NULLION_SLACK_SIGNING_SECRET": "slack-secret",
            "NULLION_SLACK_OPERATOR_USER_ID": "U1",
            "NULLION_DISCORD_ENABLED": "yes",
            "NULLION_DISCORD_BOT_TOKEN": "discord-bot",
            "NULLION_TERMINAL_BACKEND_MODE": "launcher",
            "NULLION_TERMINAL_LAUNCHER_COMMAND": "nullion-launcher",
            "NULLION_TERMINAL_LAUNCHER_ARGS": "--flag 'two words'",
            "NULLION_ALLOWED_ROOTS": "/tmp,/var/tmp",
            "NULLION_WORKSPACE_ROOT": "/workspace",
            "NULLION_REASONING_EFFORT": "medium",
        },
    )

    assert settings.telegram.chat_enabled is False
    assert settings.slack.enabled is True
    assert settings.slack.bot_token == "slack-bot"
    assert settings.discord.enabled is True
    assert settings.terminal_execution.backend_mode == "launcher"
    assert settings.terminal_execution.launcher_args == ("--flag", "two words")
    assert settings.allowed_roots == ("/tmp", "/var/tmp")
    assert settings.workspace_root == "/workspace"
    assert settings.model.reasoning_effort == "medium"

    with pytest.raises(ValueError, match="unknown terminal"):
        config.load_settings(env={"NULLION_TERMINAL_BACKEND_MODE": "bad"})
    with pytest.raises(ValueError, match="requires a launcher command"):
        config.load_settings(env={"NULLION_TERMINAL_BACKEND_MODE": "launcher"})
    with pytest.raises(ValueError, match="duplicate provider binding"):
        config.load_settings(env={"NULLION_PROVIDER_BINDINGS": "a=x,a=y"})


def test_settings_load_stored_credentials_when_env_is_not_explicit(monkeypatch, tmp_path) -> None:
    from nullion import config

    env_file = tmp_path / ".env"
    env_file.write_text("NULLION_TELEGRAM_CHAT_ENABLED=true\n", encoding="utf-8")
    env_file.chmod(0o600)
    monkeypatch.setattr(
        "nullion.auth.load_stored_credentials",
        lambda: {
            "provider": "openrouter",
            "keys": {"openrouter": "stored-key"},
            "models": {"openrouter": "stored-model"},
            "base_url": "https://stored.example/v1",
            "thinking_level": "low",
        },
    )

    settings = config.load_settings(env_path=env_file)

    assert settings.model.provider == "openrouter"
    assert settings.model.openai_api_key == "stored-key"
    assert settings.model.openai_model == "stored-model"
    assert settings.model.openai_base_url == "https://stored.example/v1"
    assert settings.model.reasoning_effort == "low"


def test_web_session_duration_helpers() -> None:
    from datetime import UTC, datetime, timedelta
    from nullion import config

    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert config.normalize_web_session_allow_duration("30mins") == "30m"
    assert config.normalize_web_session_allow_duration("bad") == "session"
    assert config.web_session_allow_duration_label("today") == "today"
    assert config.web_session_allow_expires_at("session", now=now) is None
    assert config.web_session_allow_expires_at("15m", now=now) == now + timedelta(minutes=15)
    assert config.web_session_allow_expires_at("1h", now=now) == now + timedelta(hours=1)
    assert config.web_session_allow_expires_at("today", now=now) == now + timedelta(days=1)


def test_auth_persistence_reauth_and_cli_paths(tmp_path, monkeypatch, capsys) -> None:
    from nullion import auth

    credentials_path = tmp_path / "credentials.json"
    monkeypatch.setattr(auth, "CREDENTIALS_PATH", credentials_path)
    auth._save({"provider": "openai", "api_key": "sk-test"})
    assert json.loads(credentials_path.read_text(encoding="utf-8"))["api_key"] == "sk-test"
    assert stat.S_IMODE(credentials_path.stat().st_mode) == 0o600
    assert auth.load_stored_credentials()["provider"] == "openai"

    credentials_path.write_text("{bad", encoding="utf-8")
    assert auth.load_stored_credentials() is None

    monkeypatch.setattr(
        auth,
        "codex_oauth_credentials",
        lambda: {
            "provider": "codex",
            "api_key": "access",
            "refresh_token": "refresh",
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-5.5",
        },
    )
    credentials_path.write_text(json.dumps({"provider": "codex", "model": "custom"}), encoding="utf-8")
    assert auth.reauthenticate_codex_oauth() == credentials_path
    saved = json.loads(credentials_path.read_text(encoding="utf-8"))
    assert saved["keys"]["codex"] == "access"
    assert saved["models"]["codex"] == "custom"

    token_path = tmp_path / "token"
    auth._cli_impl(["--write-codex-access-token", str(token_path)])
    assert token_path.read_text(encoding="utf-8") == "access"
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600

    auth._cli_impl(["--print-codex-access-token"])
    assert capsys.readouterr().out.endswith("access\n")

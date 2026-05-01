from __future__ import annotations

import json
import sqlite3

from nullion.credential_store import (
    credentials_from_env,
    load_encrypted_credentials,
    migrate_credentials_json_to_db,
    migrate_env_credentials_to_db,
    save_encrypted_credentials,
)


def test_encrypted_credentials_round_trip_without_plaintext(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    creds = {"provider": "openai", "api_key": "sk-test-secret", "model": "gpt-5.5"}

    save_encrypted_credentials(creds, db_path=db_path)

    assert load_encrypted_credentials(db_path=db_path) == creds
    raw = db_path.read_bytes()
    assert b"sk-test-secret" not in raw
    with sqlite3.connect(db_path) as conn:
        stored = conn.execute("SELECT payload FROM encrypted_credentials").fetchone()[0]
    assert "sk-test-secret" not in stored


def test_legacy_credentials_json_migrates_and_removes_plaintext(tmp_path) -> None:
    json_path = tmp_path / "credentials.json"
    json_path.write_text(
        json.dumps(
            {
                "provider": "codex",
                "api_key": "access-secret",
                "refresh_token": "refresh-secret",
                "model": "gpt-5.5",
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "runtime.db"

    migrated = migrate_credentials_json_to_db(json_path, db_path=db_path)

    assert migrated["provider"] == "codex"
    stored = load_encrypted_credentials(db_path=db_path)
    assert stored["refresh_token"] == "refresh-secret"
    assert stored["keys"]["codex"] == "access-secret"
    assert stored["models"]["codex"] == "gpt-5.5"
    assert not json_path.exists()
    assert b"refresh-secret" not in db_path.read_bytes()


def test_legacy_credentials_json_preserves_codex_when_existing_db_has_other_provider(tmp_path) -> None:
    json_path = tmp_path / "credentials.json"
    json_path.write_text(
        json.dumps(
            {
                "provider": "codex",
                "api_key": "codex-access",
                "refresh_token": "codex-refresh",
                "model": "gpt-5.5",
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "runtime.db"
    save_encrypted_credentials(
        {
            "provider": "openrouter",
            "api_key": "sk-or-secret",
            "model": "qwen/qwen3.6-flash",
            "keys": {"openrouter": "sk-or-secret"},
            "models": {"openrouter": "qwen/qwen3.6-flash"},
        },
        db_path=db_path,
    )

    migrated = migrate_credentials_json_to_db(json_path, db_path=db_path)

    assert migrated["provider"] == "openrouter"
    assert migrated["api_key"] == "sk-or-secret"
    assert migrated["refresh_token"] == "codex-refresh"
    assert migrated["keys"]["openrouter"] == "sk-or-secret"
    assert migrated["keys"]["codex"] == "codex-access"
    assert migrated["models"]["openrouter"] == "qwen/qwen3.6-flash"
    assert migrated["models"]["codex"] == "gpt-5.5"
    assert not json_path.exists()


def test_env_credentials_migrate_to_db_without_clobbering_existing(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    save_encrypted_credentials(
        {"provider": "codex", "api_key": "oauth-token", "model": "gpt-5.5"},
        db_path=db_path,
    )

    migrated = migrate_env_credentials_to_db(
        db_path=db_path,
        env={
            "NULLION_MODEL_PROVIDER": "openrouter",
            "NULLION_MODEL": "openai/gpt-4o",
            "OPENROUTER_API_KEY": "sk-or-secret",
        },
        overwrite=False,
    )

    assert migrated["provider"] == "codex"
    assert migrated["api_key"] == "oauth-token"
    assert migrated["keys"]["openrouter"] == "sk-or-secret"


def test_env_credentials_can_overwrite_during_fresh_install(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    save_encrypted_credentials({"provider": "openai", "api_key": "old", "model": "old-model"}, db_path=db_path)

    migrated = migrate_env_credentials_to_db(
        db_path=db_path,
        env={
            "NULLION_MODEL_PROVIDER": "gemini",
            "NULLION_MODEL": "models/gemini-2.5-flash",
            "GEMINI_API_KEY": "AIza-secret",
        },
        overwrite=True,
    )

    assert credentials_from_env({"NULLION_MODEL_PROVIDER": "gemini", "GEMINI_API_KEY": "AIza-secret"})["provider"] == "gemini"
    assert migrated["provider"] == "gemini"
    assert migrated["api_key"] == "AIza-secret"
    assert migrated["model"] == "models/gemini-2.5-flash"

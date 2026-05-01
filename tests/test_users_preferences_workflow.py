from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

from nullion import preferences, users
from nullion.preferences import (
    Preferences,
    build_preferences_prompt,
    build_profile_prompt,
    detect_system_timezone,
    load_preferences,
    load_profile,
    resolve_timezone,
    save_preferences,
)
from nullion.users import (
    NullionUser,
    UserRegistry,
    build_messaging_user_context_prompt,
    is_authorized_messaging_identity,
    is_authorized_telegram_chat,
    load_user_registry,
    messaging_delivery_targets_for_workspace,
    resolve_messaging_user,
    resolve_telegram_user,
    save_user_registry,
    workspace_id_for_user,
    workspace_users,
)


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        telegram=SimpleNamespace(operator_chat_id="admin-chat"),
        slack=SimpleNamespace(operator_user_id="admin-slack"),
    )


def test_user_registry_load_save_authorization_and_resolution(tmp_path, monkeypatch) -> None:
    path = tmp_path / "users.json"
    raw = {
        "multi_user_enabled": True,
        "users": [
            {"role": "admin", "display_name": "Boss", "telegram_chat_id": "boss-chat", "notes": "admin notes"},
            {"user_id": "u1", "display_name": "Ada Lovelace", "telegram_chat_id": "42", "notes": "math"},
            {"user_id": "u2", "display_name": "Slack User", "messaging_channel": "slack", "messaging_user_id": "U2"},
            {"user_id": "u2", "display_name": "Duplicate"},
            "drop",
        ],
    }
    path.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setattr(users, "_USERS_PATH", path)

    registry = load_user_registry(path=path, settings=settings())
    assert registry.multi_user_enabled is True
    assert registry.users[0].user_id == "admin"
    assert registry.users[0].telegram_chat_id == "boss-chat"
    assert len({user.user_id for user in registry.users}) == len(registry.users)

    save_user_registry(registry, path=path, settings=settings())
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["users"][0]["telegram_chat_id"] == "admin-chat"

    assert is_authorized_telegram_chat("admin-chat", settings()) is True
    assert is_authorized_telegram_chat("42", settings()) is True
    assert is_authorized_telegram_chat("missing", settings()) is False
    assert is_authorized_messaging_identity("slack", "admin-slack", settings()) is True
    assert is_authorized_messaging_identity("slack", "U2", settings()) is True
    assert is_authorized_messaging_identity("discord", "nobody", settings()) is False

    assert resolve_telegram_user("42", settings()).display_name == "Ada Lovelace"
    assert resolve_messaging_user("slack", "admin-slack", settings()).role == "admin"
    assert resolve_messaging_user("slack", "U2", settings()).display_name == "Slack User"
    assert resolve_messaging_user("discord", "missing", settings()).role == "admin"


def test_workspace_targets_and_context_prompt(tmp_path, monkeypatch) -> None:
    path = tmp_path / "users.json"
    registry = UserRegistry(
        multi_user_enabled=True,
        users=[
            NullionUser("admin", "Admin", role="admin", workspace_id="workspace_admin", telegram_chat_id="admin-chat"),
            NullionUser("u1", "Ada", workspace_id="workspace_ada", telegram_chat_id="42", messaging_user_id="42", notes="likes concise replies"),
            NullionUser("u2", "Ada Slack", workspace_id="workspace_ada", messaging_channel="slack", messaging_user_id="U2"),
            NullionUser("u3", "Inactive", workspace_id="workspace_ada", telegram_chat_id="99", active=False),
        ],
    )
    save_user_registry(registry, path=path, settings=settings())
    monkeypatch.setattr(users, "_USERS_PATH", path)
    monkeypatch.setenv("NULLION_DISCORD_OPERATOR_CHANNEL_ID", "discord-admin")

    assert [user.display_name for user in workspace_users("workspace_ada", settings=settings())] == ["Ada", "Ada Slack"]
    assert workspace_id_for_user(NullionUser("x", "Casey Jones")) == "workspace_casey_jones"
    targets = messaging_delivery_targets_for_workspace("workspace_ada", settings=settings())
    assert {(target.channel, target.target_id, target.principal_id) for target in targets} == {
        ("telegram", "42", "user:u1"),
        ("slack", "U2", "user:u2"),
    }
    admin_targets = messaging_delivery_targets_for_workspace("workspace_admin", settings=settings())
    assert ("discord", "discord-admin") in {(target.channel, target.target_id) for target in admin_targets}

    prompt = build_messaging_user_context_prompt("telegram", "42", settings())
    assert "Name: Ada" in prompt
    assert "Notes: likes concise replies" in prompt
    assert "Treat this person as the current end user" in prompt


def test_preferences_load_save_prompt_profile_and_timezone(tmp_path, monkeypatch) -> None:
    prefs_path = tmp_path / "preferences.json"
    profile_path = tmp_path / "profile.json"
    monkeypatch.setattr(preferences, "_PREFS_PATH", prefs_path)
    monkeypatch.setattr(preferences, "_PROFILE_PATH", profile_path)
    monkeypatch.setenv("NULLION_TIMEZONE", "America/New_York")

    assert detect_system_timezone() == "America/New_York"
    assert resolve_timezone("No/SuchZone") is UTC

    prefs_path.write_text(
        json.dumps(
            {
                "persona": "x" * 400,
                "sentinel_risk_level": 99,
                "sentinel_mode": "bad",
                "outbound_request_mode": "bad",
                "approval_strictness": "bad",
                "timezone": "UTC",
                "emoji_level": "none",
                "response_length": "concise",
                "response_structure": "numbered",
                "markdown_style": "plain",
                "tone": "formal",
                "language_complexity": "technical",
                "code_examples": "never",
                "proactive_suggestions": False,
                "confirm_before_action": True,
                "show_reasoning": True,
            }
        ),
        encoding="utf-8",
    )
    loaded = load_preferences()
    assert len(loaded.persona) == 280
    assert loaded.sentinel_risk_level == 10
    assert loaded.sentinel_mode == "risk_based"
    assert loaded.approval_strictness == "balanced"

    prompt = build_preferences_prompt(loaded, now=datetime(2026, 1, 1, 12, 0, tzinfo=UTC))
    assert "Use no emojis whatsoever" in prompt
    assert "Always confirm with the user" in prompt
    assert "Current UTC time: 2026-01-01T12:00:00+00:00" in prompt

    save_preferences(Preferences(persona="hello", timezone="UTC"))
    assert json.loads(prefs_path.read_text(encoding="utf-8"))["persona"] == "hello"

    assert load_profile() == {}
    profile_path.write_text(json.dumps({"name": "Himan", "unknown": "drop", "email": " h@example.com "}), encoding="utf-8")
    assert load_profile() == {"name": "Himan", "email": "h@example.com"}
    assert build_profile_prompt({"name": "Himan", "notes": ""}) == "User profile:\nName: Himan"
    assert build_profile_prompt({}) is None

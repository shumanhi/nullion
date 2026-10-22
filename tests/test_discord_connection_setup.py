from types import SimpleNamespace

import pytest
from nullion import users
from nullion.config import DiscordSettings, NullionSettings, load_settings
from nullion.discord_app import (
    _require_discord_settings,
    discord_authorized_user_configured,
)
from nullion.messaging_adapters import MessagingAdapterConfigurationError


@pytest.fixture
def discord_setup(tmp_path, monkeypatch):
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "users.json")
    return NullionSettings(
        discord=DiscordSettings(
            enabled=True, bot_token="fixture-bot-token", operator_user_id="2001"
        )
    )


def test_operator_identity_loads_and_authorizes_only_explicit_sender(discord_setup):
    s = load_settings(
        env={
            "NULLION_DISCORD_ENABLED": "true",
            "NULLION_DISCORD_BOT_TOKEN": "fixture-token",
            "NULLION_DISCORD_OPERATOR_USER_ID": "2001",
        }
    )
    assert s.discord.operator_user_id == "2001"
    assert users.is_authorized_messaging_identity("discord", "2001", s)
    assert users.resolve_messaging_user("discord", "2001", s).role == "admin"
    for identity in ("2002", "", None):
        assert not users.is_authorized_messaging_identity("discord", identity, s)
    assert _require_discord_settings(s) == "fixture-token"


@pytest.mark.parametrize(
    "identity", ["name#1234", "<@2001>", "0", str(2**64), "٢", "02001", "9" * 5000]
)
def test_invalid_operator_id_is_a_configuration_error(discord_setup, identity):
    discord_setup.discord.operator_user_id = identity
    with pytest.raises(MessagingAdapterConfigurationError, match="numeric user ID"):
        _require_discord_settings(discord_setup)


def test_token_alone_does_not_claim_a_usable_configuration(discord_setup):
    discord_setup.discord.operator_user_id = None
    assert not discord_authorized_user_configured(discord_setup)
    with pytest.raises(MessagingAdapterConfigurationError, match="no authorized user"):
        _require_discord_settings(discord_setup)


@pytest.mark.parametrize(
    "active,multi_user,expected",
    [(True, True, True), (False, True, False), (True, False, False)],
)
def test_registered_member_only_setup_still_works(
    discord_setup, active, multi_user, expected
):
    discord_setup.discord.operator_user_id = None
    users.save_user_registry(
        {
            "multi_user_enabled": multi_user,
            "users": [
                {
                    "user_id": "member",
                    "display_name": "Member",
                    "messaging_channel": "discord",
                    "messaging_user_id": "2002",
                    "active": active,
                }
            ],
        },
        settings=discord_setup,
    )
    assert discord_authorized_user_configured(discord_setup) == expected
    assert (
        users.is_authorized_messaging_identity("discord", "2002", discord_setup)
        == expected
    )
    if expected:
        assert _require_discord_settings(discord_setup) == "fixture-bot-token"
    else:
        with pytest.raises(MessagingAdapterConfigurationError):
            _require_discord_settings(discord_setup)


@pytest.mark.asyncio
async def test_unregistered_attachments_are_not_downloaded(discord_setup, monkeypatch):
    from nullion import discord_app

    sent = []

    class Channel:
        async def send(self, text):
            sent.append(text)

    async def forbidden_download(*args, **kwargs):
        pytest.fail("Unauthorized attachment download")

    monkeypatch.setattr(
        discord_app, "_download_discord_attachments", forbidden_download
    )
    m = SimpleNamespace(
        author=SimpleNamespace(id=2002, bot=False),
        content="文件",
        attachments=[object()],
        channel=Channel(),
        id=5001,
    )
    await discord_app.handle_discord_message(SimpleNamespace(), discord_setup, m)
    assert len(sent) == 1 and "not authorized" in sent[0]


def test_missing_discord_settings_on_legacy_settings_object_still_denies():
    s = SimpleNamespace(
        telegram=SimpleNamespace(operator_chat_id=None),
        slack=SimpleNamespace(operator_user_id=None),
    )
    assert not users.is_authorized_messaging_identity("discord", "2002", s)


def test_cli_configuration_error_is_clear_and_has_no_traceback(monkeypatch, capsys):
    from nullion import discord_app

    def bad_setup(*args, **kwargs):
        raise MessagingAdapterConfigurationError("Discord has no authorized user.")

    monkeypatch.setattr(discord_app, "run_user_facing_entrypoint", bad_setup)
    with pytest.raises(SystemExit) as error:
        discord_app.cli()
    assert error.value.code == 2
    assert capsys.readouterr().err == "Discord setup: Discord has no authorized user.\n"

from __future__ import annotations

from nullion.web_app import _model_provider_and_codex_token_for_config


def test_codex_oauth_install_shape_counts_as_connected(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_CODEX_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("CODEX_REFRESH_TOKEN", raising=False)

    provider, token = _model_provider_and_codex_token_for_config(
        "codex",
        oai_key="oauth-access-token",
        creds={"provider": "codex"},
        stored_keys={},
    )

    assert provider == "codex"
    assert token == "oauth-access-token"


def test_legacy_openai_oauth_shape_is_shown_as_connected_codex(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_CODEX_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("CODEX_REFRESH_TOKEN", raising=False)

    provider, token = _model_provider_and_codex_token_for_config(
        "openai",
        oai_key="oauth-access-token",
        creds={},
        stored_keys={},
    )

    assert provider == "codex"
    assert token == "oauth-access-token"


def test_platform_openai_key_stays_openai_not_codex(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_CODEX_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("CODEX_REFRESH_TOKEN", raising=False)

    provider, token = _model_provider_and_codex_token_for_config(
        "openai",
        oai_key="sk-platform-key",
        creds={},
        stored_keys={},
    )

    assert provider == "openai"
    assert token == ""


def test_codex_refresh_token_env_takes_precedence_over_access_token(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_CODEX_REFRESH_TOKEN", "refresh-token")
    monkeypatch.delenv("CODEX_REFRESH_TOKEN", raising=False)

    provider, token = _model_provider_and_codex_token_for_config(
        "codex",
        oai_key="oauth-access-token",
        creds={"provider": "codex", "api_key": "stored-access-token"},
        stored_keys={"codex": "stored-key-token"},
    )

    assert provider == "codex"
    assert token == "refresh-token"


def test_stored_codex_key_counts_as_connected_when_env_is_empty(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_CODEX_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("CODEX_REFRESH_TOKEN", raising=False)

    provider, token = _model_provider_and_codex_token_for_config(
        "codex",
        oai_key="",
        creds={},
        stored_keys={"codex": "stored-key-token"},
    )

    assert provider == "codex"
    assert token == "stored-key-token"

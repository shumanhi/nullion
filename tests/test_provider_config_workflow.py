from __future__ import annotations

import pytest

from nullion.config import load_settings


OPENAI_COMPAT_PROVIDERS = {
    "openrouter": ("NULLION_OPENROUTER_API_KEY", "or-key", "https://openrouter.ai/api/v1"),
    "gemini": ("NULLION_GEMINI_API_KEY", "gemini-key", "https://generativelanguage.googleapis.com/v1beta/openai/"),
    "groq": ("NULLION_GROQ_API_KEY", "groq-key", "https://api.groq.com/openai/v1"),
    "mistral": ("NULLION_MISTRAL_API_KEY", "mistral-key", "https://api.mistral.ai/v1"),
    "deepseek": ("NULLION_DEEPSEEK_API_KEY", "deepseek-key", "https://api.deepseek.com/v1"),
    "xai": ("NULLION_XAI_API_KEY", "xai-key", "https://api.x.ai/v1"),
    "together": ("NULLION_TOGETHER_API_KEY", "together-key", "https://api.together.xyz/v1"),
}


@pytest.mark.parametrize("provider, env_spec", OPENAI_COMPAT_PROVIDERS.items())
def test_openai_compatible_provider_envs_resolve_key_base_url_and_first_model(provider, env_spec) -> None:
    key_env, key_value, base_url = env_spec
    settings = load_settings(
        env={
            "NULLION_MODEL_PROVIDER": provider,
            "NULLION_MODEL": "primary-model, fallback-model",
            key_env: key_value,
        }
    )

    assert settings.model.provider == provider
    assert settings.model.openai_api_key == key_value
    assert settings.model.openai_base_url == base_url
    assert settings.model.openai_model == "primary-model"


def test_ollama_provider_defaults_to_local_key_and_base_url() -> None:
    settings = load_settings(env={"NULLION_MODEL_PROVIDER": "ollama", "NULLION_MODEL": "llama3.3"})

    assert settings.model.provider == "ollama"
    assert settings.model.openai_api_key == "ollama-local"
    assert settings.model.openai_base_url == "http://127.0.0.1:11434/v1"
    assert settings.model.openai_model == "llama3.3"


def test_anthropic_provider_uses_anthropic_key_env() -> None:
    settings = load_settings(
        env={
            "NULLION_MODEL_PROVIDER": "anthropic",
            "ANTHROPIC_API_KEY": "sk-ant-test",
            "NULLION_MODEL": "claude-opus-4-6",
        }
    )

    assert settings.model.provider == "anthropic"
    assert settings.model.openai_api_key == "sk-ant-test"
    assert settings.model.openai_model == "claude-opus-4-6"


def test_openai_non_sk_token_is_migrated_to_codex_for_legacy_oauth_installs() -> None:
    settings = load_settings(
        env={
            "NULLION_MODEL_PROVIDER": "openai",
            "OPENAI_API_KEY": "oauth-access-token",
            "NULLION_MODEL": "gpt-5.5",
        }
    )

    assert settings.model.provider == "codex"
    assert settings.model.openai_api_key == "oauth-access-token"


def test_provider_bindings_plugins_skill_packs_and_web_allow_duration_parse() -> None:
    settings = load_settings(
        env={
            "NULLION_ENABLED_PLUGINS": "search_plugin,browser_plugin,workspace_plugin",
            "NULLION_PROVIDER_BINDINGS": "search_plugin=builtin_search_provider,email_plugin=custom_api_provider",
            "NULLION_ENABLED_SKILL_PACKS": "nullion/web-research,nullion/connector-skills",
            "NULLION_WEB_SESSION_ALLOW_DURATION": "30min",
        }
    )

    assert settings.enabled_plugins == ("search_plugin", "browser_plugin", "workspace_plugin")
    assert [(binding.capability, binding.provider) for binding in settings.provider_bindings] == [
        ("search_plugin", "builtin_search_provider"),
        ("email_plugin", "custom_api_provider"),
    ]
    assert settings.enabled_skill_packs == ("nullion/web-research", "nullion/connector-skills")
    assert settings.web_session_allow_duration == "30m"

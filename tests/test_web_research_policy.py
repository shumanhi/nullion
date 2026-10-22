from __future__ import annotations

from types import SimpleNamespace

import pytest

from nullion import web_research_policy


def _settings(provider: str | None = None):
    bindings = []
    if provider is not None:
        bindings.append(SimpleNamespace(capability="search_plugin", provider=provider))
    return SimpleNamespace(provider_bindings=bindings)


class _Registry:
    def __init__(self, *names: str) -> None:
        self._names = names

    def list_specs(self):
        return [SimpleNamespace(name=name) for name in self._names]


def test_api_backed_search_requires_enabled_web_access_and_credentials(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "1")
    monkeypatch.setenv("NULLION_DIRECT_WEB_FETCH_ENABLED", "0")
    monkeypatch.delenv("NULLION_BRAVE_SEARCH_API_KEY", raising=False)
    monkeypatch.delenv("BRAVE_SEARCH_API_KEY", raising=False)

    settings = _settings("brave_search_provider")

    assert web_research_policy.configured_search_provider(settings) == "brave_search_provider"
    assert web_research_policy.should_register_search_plugin(settings) is False
    assert web_research_policy.default_browser_backend_for_web_research(settings) == "playwright"

    monkeypatch.setenv("NULLION_BRAVE_SEARCH_API_KEY", "brave-test-key")
    assert web_research_policy.should_register_search_plugin(settings) is True

    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "0")
    assert web_research_policy.should_register_search_plugin(settings) is False
    assert web_research_policy.default_browser_backend_for_web_research(settings) is None


def test_direct_fetch_override_disables_default_browser_backend(monkeypatch) -> None:
    settings = _settings("brave_search_provider")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "1")
    monkeypatch.setenv("NULLION_DIRECT_WEB_FETCH_ENABLED", "1")

    assert web_research_policy.direct_web_fetch_enabled(settings) is True
    assert web_research_policy.default_browser_backend_for_web_research(settings) is None

    monkeypatch.setenv("NULLION_DIRECT_WEB_FETCH_ENABLED", "0")
    assert web_research_policy.direct_web_fetch_enabled(settings) is False
    assert web_research_policy.default_browser_backend_for_web_research(settings) == "playwright"


def test_preferred_browser_backend_requires_configured_browser_plugin(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "1")
    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "brave")
    monkeypatch.delenv("NULLION_PLUGINS", raising=False)
    monkeypatch.setenv("NULLION_ENABLED_PLUGINS", "search_plugin,workspace_plugin")

    assert web_research_policy.preferred_browser_backend_from_env() is None

    monkeypatch.setenv("NULLION_ENABLED_PLUGINS", "search_plugin,browser_plugin,workspace_plugin")
    assert web_research_policy.preferred_browser_backend_from_env() == "auto"

    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "0")
    assert web_research_policy.preferred_browser_backend_from_env() is None


def test_web_research_guidance_distinguishes_api_search_from_browser_fallback(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_BRAVE_SEARCH_API_KEY", "brave-test-key")
    api_guidance = web_research_policy.format_web_research_guidance(
        tool_registry=_Registry("browser_navigate", "web_search"),
        settings=_settings("brave_search_provider"),
    )

    assert api_guidance is not None
    assert "Use web_search for source discovery" in api_guidance
    assert "setup tip" not in api_guidance.lower()

    monkeypatch.delenv("NULLION_BRAVE_SEARCH_API_KEY", raising=False)
    fallback_guidance = web_research_policy.format_web_research_guidance(
        tool_registry=_Registry("browser_navigate"),
        settings=_settings("brave_search_provider"),
    )

    assert fallback_guidance is not None
    assert "without usable API credentials" in fallback_guidance
    assert "browser_navigate" in fallback_guidance
    assert "💡 Setup tip:" in fallback_guidance
    assert "Brave Search API" in fallback_guidance


@pytest.mark.parametrize(
    ("package", "requirement", "message"),
    [
        ("../local", None, "PyPI package name"),
        ("safe-package", "other-package>=1", "must match"),
        ("safe-package", "safe-package; rm -rf /", "PyPI requirement"),
    ],
)
def test_custom_capability_dependency_rejects_unsafe_package_specs(
    package: str,
    requirement: str | None,
    message: str,
) -> None:
    from nullion.capability_dependencies import normalize_custom_dependency

    with pytest.raises(ValueError, match=message):
        normalize_custom_dependency(package=package, requirement=requirement)


def test_custom_github_dependency_is_normalized_without_credentials_or_path_escape() -> None:
    from nullion.capability_dependencies import normalize_custom_dependency

    dependency = normalize_custom_dependency(
        package="acme-tools",
        github_url="https://github.com/acme/tools/tree/main",
        git_ref="v1.2.3",
        subdirectory="python/pkg",
    )

    assert dependency.requirement == (
        "acme-tools @ git+https://github.com/acme/tools.git@v1.2.3#subdirectory=python/pkg"
    )
    assert dependency.install_command == "python -m pip install 'acme-tools @ git+https://github.com/acme/tools.git@v1.2.3#subdirectory=python/pkg'"

    with pytest.raises(ValueError, match="must not include credentials"):
        normalize_custom_dependency(
            package="acme-tools",
            github_url="https://token@github.com/acme/tools",
        )
    with pytest.raises(ValueError, match="Only public github.com"):
        normalize_custom_dependency(
            package="acme-tools",
            github_url="https://gitlab.com/acme/tools",
        )
    with pytest.raises(ValueError, match="Git ref"):
        normalize_custom_dependency(
            package="acme-tools",
            github_url="https://github.com/acme/tools",
            git_ref="../main",
        )

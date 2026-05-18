"""Runtime policy for public web research tool selection."""

from __future__ import annotations

import os
from typing import Any

from nullion.tips import WEB_RESEARCH_SETUP_TIP, setup_tip_instruction


_API_BACKED_SEARCH_PROVIDERS = {
    "brave_search_provider",
    "google_custom_search_provider",
    "google_search_provider",
    "perplexity_search_provider",
}
_KEYLESS_SEARCH_PROVIDERS = {
    "builtin_search_provider",
    "duckduckgo_instant_answer_provider",
    "duckduckgo_search_provider",
}


def _truthy_env(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"", "0", "false", "no", "off"}


def configured_search_provider(settings: Any) -> str | None:
    for binding in getattr(settings, "provider_bindings", ()) or ():
        if getattr(binding, "capability", None) == "search_plugin":
            provider = str(getattr(binding, "provider", "") or "").strip()
            return provider or None
    return None


def search_provider_has_api_credentials(provider_name: str | None) -> bool:
    provider = str(provider_name or "").strip()
    if provider == "brave_search_provider":
        return bool(os.environ.get("NULLION_BRAVE_SEARCH_API_KEY") or os.environ.get("BRAVE_SEARCH_API_KEY"))
    if provider in {"google_custom_search_provider", "google_search_provider"}:
        api_key = os.environ.get("NULLION_GOOGLE_SEARCH_API_KEY") or os.environ.get("GOOGLE_SEARCH_API_KEY")
        cx = os.environ.get("NULLION_GOOGLE_SEARCH_CX") or os.environ.get("GOOGLE_SEARCH_CX")
        return bool(api_key and cx)
    if provider == "perplexity_search_provider":
        return bool(os.environ.get("NULLION_PERPLEXITY_API_KEY") or os.environ.get("PERPLEXITY_API_KEY"))
    return False


def web_access_enabled() -> bool:
    return _truthy_env("NULLION_WEB_ACCESS_ENABLED", default=True)


def api_backed_search_configured(settings: Any) -> bool:
    if not web_access_enabled():
        return False
    provider = configured_search_provider(settings)
    return provider in _API_BACKED_SEARCH_PROVIDERS and search_provider_has_api_credentials(provider)


def direct_web_fetch_enabled(settings: Any) -> bool:
    if not web_access_enabled():
        return False
    override = os.environ.get("NULLION_DIRECT_WEB_FETCH_ENABLED")
    if override is None:
        return False
    return _truthy_env("NULLION_DIRECT_WEB_FETCH_ENABLED")


def browser_disabled() -> bool:
    return not _truthy_env("NULLION_BROWSER_ENABLED", default=True)


def should_register_search_plugin(settings: Any) -> bool:
    if not web_access_enabled():
        return False
    provider = configured_search_provider(settings)
    if provider in _KEYLESS_SEARCH_PROVIDERS:
        return True
    return api_backed_search_configured(settings)


def default_browser_backend_for_web_research(settings: Any) -> str | None:
    if not web_access_enabled():
        return None
    if browser_disabled():
        return None
    if direct_web_fetch_enabled(settings):
        return None
    return "playwright"


def format_web_research_guidance(*, tool_registry: Any, settings: Any) -> str | None:
    spec_names = {
        str(getattr(spec, "name", "") or "")
        for spec in (getattr(tool_registry, "list_specs", lambda: [])() or ())
    }
    if "browser_navigate" not in spec_names:
        return None
    if api_backed_search_configured(settings):
        if "web_fetch" in spec_names:
            return None
        return (
            "Public web research mode:\n"
            "- Use web_search for source discovery and browser_navigate, browser_extract_text, and browser_screenshot for page evidence.\n"
            "- Direct web_fetch is not registered in this session."
        )
    provider = configured_search_provider(settings) or "not configured"
    return (
        "Public web research mode:\n"
        f"- Search API provider is {provider}, without usable API credentials; direct web_fetch/web_search tools are not registered.\n"
        "- Use browser_navigate, browser_extract_text, and browser_screenshot for public web evidence.\n"
        "- When browser research is used for the final answer, "
        f"{setup_tip_instruction(WEB_RESEARCH_SETUP_TIP)}"
    )

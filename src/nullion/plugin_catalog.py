"""Product-facing plugin catalog for Nullion capabilities."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PluginProviderCatalogEntry:
    provider_id: str
    name: str
    status: str
    notes: str


@dataclass(frozen=True, slots=True)
class PluginCatalogEntry:
    plugin_id: str
    name: str
    category: str
    status: str
    summary: str
    tools: tuple[str, ...]
    providers: tuple[PluginProviderCatalogEntry, ...] = ()
    setup_hint: str = ""


PLUGIN_CATALOG: tuple[PluginCatalogEntry, ...] = (
    PluginCatalogEntry(
        plugin_id="search_plugin",
        name="Search",
        category="Knowledge",
        status="available",
        summary="Search the web and fetch public pages with Sentinel approval on outbound access.",
        tools=("web_search", "web_fetch"),
        providers=(
            PluginProviderCatalogEntry(
                provider_id="builtin_search_provider",
                name="Built-in web search",
                status="available",
                notes="Default local provider adapter for search/fetch.",
            ),
            PluginProviderCatalogEntry(
                provider_id="brave_search_provider",
                name="Brave Search",
                status="available",
                notes="Uses the Brave Search API with NULLION_BRAVE_SEARCH_API_KEY.",
            ),
            PluginProviderCatalogEntry(
                provider_id="google_custom_search_provider",
                name="Google Custom Search",
                status="available",
                notes=(
                    "Uses Google's Custom Search JSON API with NULLION_GOOGLE_SEARCH_API_KEY "
                    "and NULLION_GOOGLE_SEARCH_CX."
                ),
            ),
            PluginProviderCatalogEntry(
                provider_id="perplexity_search_provider",
                name="Perplexity Search",
                status="available",
                notes="Uses Perplexity's Search API with NULLION_PERPLEXITY_API_KEY.",
            ),
            PluginProviderCatalogEntry(
                provider_id="duckduckgo_instant_answer_provider",
                name="DuckDuckGo Instant Answers",
                status="available",
                notes="Keyless instant-answer lookup. Not a full organic web search API.",
            ),
        ),
        setup_hint=(
            "Enable search_plugin and bind it to builtin_search_provider, brave_search_provider, "
            "google_custom_search_provider, perplexity_search_provider, or duckduckgo_instant_answer_provider."
        ),
    ),
    PluginCatalogEntry(
        plugin_id="browser_plugin",
        name="Browser",
        category="Automation",
        status="available",
        summary="Open and inspect real browser pages, capture screenshots, and keep an agent browser session.",
        tools=("browser_open", "browser_navigate", "browser_snapshot", "browser_screenshot"),
        providers=(
            PluginProviderCatalogEntry(
                provider_id="playwright_browser_provider",
                name="Playwright",
                status="available",
                notes="Local browser automation backend.",
            ),
            PluginProviderCatalogEntry(
                provider_id="cdp_browser_provider",
                name="Chrome/Brave CDP",
                status="available",
                notes="Connects to a configured local browser debug session.",
            ),
        ),
        setup_hint="Set NULLION_BROWSER_ENABLED=true and choose NULLION_BROWSER_BACKEND=auto, playwright, or cdp.",
    ),
    PluginCatalogEntry(
        plugin_id="workspace_plugin",
        name="Workspace",
        category="Files",
        status="available",
        summary="Read, search, and write files inside explicitly allowed local folders.",
        tools=("file_search", "file_patch", "workspace_summary"),
        setup_hint="Enable workspace_plugin and set NULLION_WORKSPACE_ROOT or NULLION_ALLOWED_ROOTS.",
    ),
    PluginCatalogEntry(
        plugin_id="media_plugin",
        name="Media",
        category="Local AI",
        status="available",
        summary=(
            "Transcribe audio, extract text from images, and optionally generate images through "
            "local open-source provider tools."
        ),
        tools=("audio_transcribe", "image_extract_text", "image_generate"),
        providers=(
            PluginProviderCatalogEntry(
                provider_id="local_media_provider",
                name="Local media tools",
                status="available",
                notes=(
                    "Adapter for local command-line providers such as whisper.cpp, faster-whisper, "
                    "Tesseract, PaddleOCR, ComfyUI, or Stable Diffusion runners."
                ),
            ),
        ),
        setup_hint=(
            "Enable media_plugin, bind it to local_media_provider, and configure the local command "
            "templates for the media tools you want."
        ),
    ),
    PluginCatalogEntry(
        plugin_id="email_plugin",
        name="Email",
        category="Accounts",
        status="preview",
        summary="Search and read email through an explicitly connected provider account.",
        tools=("email_search", "email_read"),
        providers=(
            PluginProviderCatalogEntry(
                provider_id="google_workspace_provider",
                name="Google Workspace / Gmail",
                status="preview",
                notes="Local provider adapter. Email and calendar stay separate capability choices.",
            ),
            PluginProviderCatalogEntry(
                provider_id="microsoft_365_provider",
                name="Microsoft 365 / Outlook",
                status="planned",
                notes="Planned provider adapter.",
            ),
            PluginProviderCatalogEntry(
                provider_id="imap_smtp_provider",
                name="IMAP / SMTP",
                status="preview",
                notes="Standards-based provider adapter for email_search and email_read.",
            ),
            PluginProviderCatalogEntry(
                provider_id="custom_api_provider",
                name="Custom HTTP API",
                status="preview",
                notes=(
                    "Calls /email/search and /email/read/{id} on NULLION_CUSTOM_API_BASE_URL "
                    "with a bearer token resolved from the workspace connection reference."
                ),
            ),
        ),
        setup_hint="Enable email_plugin and bind it to google_workspace_provider, imap_smtp_provider, or custom_api_provider.",
    ),
    PluginCatalogEntry(
        plugin_id="calendar_plugin",
        name="Calendar",
        category="Accounts",
        status="preview",
        summary="List calendar events through an explicitly connected provider account.",
        tools=("calendar_list",),
        providers=(
            PluginProviderCatalogEntry(
                provider_id="google_workspace_provider",
                name="Google Calendar",
                status="preview",
                notes="Same provider family as Gmail, but calendar_plugin is enabled separately.",
            ),
            PluginProviderCatalogEntry(
                provider_id="microsoft_365_provider",
                name="Microsoft 365 Calendar",
                status="planned",
                notes="Planned provider adapter.",
            ),
            PluginProviderCatalogEntry(
                provider_id="apple_calendar_provider",
                name="Apple Calendar",
                status="planned",
                notes="Planned local/calendar-account provider adapter.",
            ),
        ),
        setup_hint="Enable calendar_plugin and bind it to a provider such as google_workspace_provider.",
    ),
    PluginCatalogEntry(
        plugin_id="messaging_plugin",
        name="Messaging",
        category="Communication",
        status="planned",
        summary="Send and read messages through connected chat/SMS providers.",
        tools=("message_search", "message_send"),
        providers=(
            PluginProviderCatalogEntry(
                provider_id="telegram_provider",
                name="Telegram",
                status="available as adapter",
                notes="Telegram is currently a chat adapter; plugin-style account actions are planned.",
            ),
            PluginProviderCatalogEntry(
                provider_id="slack_provider",
                name="Slack",
                status="available as adapter",
                notes="Slack is currently a chat adapter; plugin-style account actions are planned.",
            ),
            PluginProviderCatalogEntry(
                provider_id="discord_provider",
                name="Discord",
                status="available as adapter",
                notes="Discord is currently a chat adapter; plugin-style account actions are planned.",
            ),
            PluginProviderCatalogEntry(
                provider_id="twilio_provider",
                name="Twilio SMS",
                status="planned",
                notes="Planned provider adapter.",
            ),
        ),
        setup_hint="Messaging account plugins are planned; Telegram, Slack, and Discord chat control are available today.",
    ),
)


def list_plugin_catalog() -> tuple[PluginCatalogEntry, ...]:
    return PLUGIN_CATALOG


def get_plugin_catalog_entry(plugin_id: str) -> PluginCatalogEntry | None:
    normalized = plugin_id.strip().lower()
    for entry in PLUGIN_CATALOG:
        if entry.plugin_id == normalized:
            return entry
    return None


def supported_plugin_ids() -> tuple[str, ...]:
    return tuple(entry.plugin_id for entry in PLUGIN_CATALOG if entry.status in {"available", "preview"})


__all__ = [
    "PLUGIN_CATALOG",
    "PluginCatalogEntry",
    "PluginProviderCatalogEntry",
    "get_plugin_catalog_entry",
    "list_plugin_catalog",
    "supported_plugin_ids",
]

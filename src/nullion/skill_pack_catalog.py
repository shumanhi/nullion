"""Product-facing catalog for external Agent Skill packs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from nullion.skill_pack_installer import BUILTIN_SKILL_PACK_PROMPTS, list_installed_skill_packs


@dataclass(frozen=True, slots=True)
class SkillPackAuthProvider:
    provider_id: str
    name: str
    credential_policy: str = "workspace"
    shared_allowed: bool = True
    notes: str = ""


@dataclass(frozen=True, slots=True)
class SkillPackCatalogEntry:
    pack_id: str
    name: str
    source_url: str
    status: str
    summary: str
    coverage: tuple[str, ...]
    setup_hint: str = ""
    required_tools: tuple[str, ...] = ()
    auth_providers: tuple[SkillPackAuthProvider, ...] = ()

    @property
    def requires_auth(self) -> bool:
        return bool(self.auth_providers)


SKILL_PACK_CATALOG: tuple[SkillPackCatalogEntry, ...] = (
    SkillPackCatalogEntry(
        pack_id="nullion/web-research",
        name="Web Research",
        source_url="built-in",
        status="built-in",
        summary="Research, web search/fetch, source review, and current-information workflows.",
        coverage=(
            "Search and fetch public web pages",
            "Compare sources and summarize findings",
            "Use citations and call out uncertainty",
        ),
        setup_hint="Enable search_plugin for live web search and browser_plugin for interactive sites.",
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/browser-automation",
        name="Browser Automation",
        source_url="built-in",
        status="built-in",
        summary="Navigate websites, test local web apps, fill forms, and capture screenshots with approvals.",
        coverage=(
            "Browser navigation and UI inspection",
            "Form-filling and screenshot verification",
            "Localhost and dashboard testing",
        ),
        setup_hint="Enable browser_plugin and pick Brave, Chrome, or headless browser mode during setup.",
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/files-and-docs",
        name="Files and Documents",
        source_url="built-in",
        status="built-in",
        summary="Work with local files, notes, reports, spreadsheets, slide decks, and document artifacts.",
        coverage=(
            "Read and edit local workspace files",
            "Draft and revise documents, spreadsheets, and presentations",
            "Preserve unrelated user changes",
        ),
        setup_hint="Configure workspace roots and enable file access before using file-editing workflows.",
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/pdf-documents",
        name="PDF Documents",
        source_url="built-in",
        status="built-in",
        summary="Create, convert, verify, and deliver real PDF artifacts instead of HTML or path-only replies.",
        coverage=(
            "PDF report and document generation",
            "HTML/Markdown/document conversion into PDF",
            "Attachment verification before completion claims",
        ),
        setup_hint="Use with file access and browser/media tools when rendering or image-heavy PDF output is needed.",
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/email-calendar",
        name="Email and Calendar",
        source_url="built-in",
        status="built-in",
        summary="Inbox triage, drafted replies, meeting prep, scheduling, reminders, and calendar summaries.",
        coverage=(
            "Gmail/Google Workspace style workflows",
            "Calendar planning and reminders",
            "Safe message drafting and review before send",
        ),
        setup_hint="Enable email_plugin/calendar_plugin and connect a provider account before account access.",
        required_tools=("email_search", "email_read", "calendar_list"),
        auth_providers=(
            SkillPackAuthProvider(
                provider_id="google_workspace_provider",
                name="Gmail / Google Workspace",
                credential_policy="workspace",
                shared_allowed=False,
                notes="Use a local Himalaya account profile for each workspace/user that should access mail.",
            ),
            SkillPackAuthProvider(
                provider_id="custom_api_provider",
                name="Custom Email API bridge",
                credential_policy="admin_decides",
                notes="Use when a bridge exposes /email/search and /email/read/{id}.",
            ),
            SkillPackAuthProvider(
                provider_id="imap_smtp_provider",
                name="IMAP / SMTP",
                credential_policy="workspace",
                shared_allowed=False,
                notes="Use app passwords or account-specific credentials per workspace.",
            ),
        ),
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/github-code",
        name="GitHub and Code Review",
        source_url="built-in",
        status="built-in",
        summary="Repository work, code review, issues, pull requests, release notes, and CI triage.",
        coverage=(
            "Local git-aware coding workflows",
            "Issue and PR planning",
            "Test and release checklist guidance",
        ),
        setup_hint="Use with file/terminal access for local repos; configure GitHub access separately for remote operations.",
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/media-local",
        name="Local Media",
        source_url="built-in",
        status="built-in",
        summary="Audio transcription, image OCR, image understanding, and local image-generation workflows.",
        coverage=(
            "whisper.cpp audio transcription",
            "Tesseract/PaddleOCR/docTR image text extraction",
            "Local image-generation command wrappers",
        ),
        setup_hint="Enable media_plugin and configure local provider commands or model-backed media defaults.",
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/productivity-memory",
        name="Productivity and Memory",
        source_url="built-in",
        status="built-in",
        summary="Task planning, daily summaries, recurring workflows, durable preferences, and follow-up organization.",
        coverage=(
            "Task planning and recurring routines",
            "Preference and memory hygiene",
            "Follow-ups and reminders",
        ),
        setup_hint="Enable memory, reminders, and scheduled tasks according to the access level you want.",
    ),
    SkillPackCatalogEntry(
        pack_id="nullion/connector-skills",
        name="Connector/API Skills",
        source_url="built-in",
        status="built-in",
        summary="Guidance for SaaS/API connector gateways, MCP workflows, and custom HTTP bridges.",
        coverage=(
            "API gateway and MCP workflows",
            "Connector credentials and account authorization checks",
            "Custom HTTP bridge patterns for services without native plugins",
        ),
        setup_hint=(
            "Set connector credentials as env var references and add native plugins or custom bridge endpoints separately."
        ),
        required_tools=("connector_request",),
        auth_providers=(
            SkillPackAuthProvider(
                provider_id="custom_connector_provider",
                name="Generic connector / MCP gateway",
                credential_policy="admin_decides",
                notes="Use for connector skills that need a gateway token or base URL.",
            ),
        ),
    ),
    SkillPackCatalogEntry(
        pack_id="google/skills",
        name="Google Skills",
        source_url="https://github.com/google/skills",
        status="available",
        summary=(
            "Reference Agent Skills for Google products and technologies. "
            "They add product workflow knowledge, not Google account access by themselves."
        ),
        coverage=(
            "Gemini API in Agent Platform",
            "BigQuery, Cloud Run, Cloud SQL, Firebase, GKE, and AlloyDB basics",
            "Google Cloud onboarding, authentication, and network observability recipes",
            "Google Cloud Well-Architected Framework: security, reliability, and cost optimization",
        ),
        setup_hint="Set NULLION_ENABLED_SKILL_PACKS=google/skills. Add plugins separately for account or browser access.",
    ),
)


_AUTH_PATTERN = re.compile(
    r"\b(api[-_\s]?key|auth(?:entication|orization)?|oauth|token|secret|credential|bearer|login)\b",
    re.IGNORECASE,
)


def _provider_id_looks_external_connector(provider_id: object) -> bool:
    normalized = str(provider_id or "").strip().lower()
    return normalized.startswith("skill_pack_connector_") or normalized.endswith("_connector_provider")


def _provider_id_for_pack(pack_id: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", pack_id.strip().lower()).strip("_") or "custom_skill"
    return f"skill_pack_connector_{slug}"


def _installed_pack_requires_auth(pack) -> bool:
    pack_path = Path(str(getattr(pack, "path", "") or ""))
    if not pack_path.exists():
        return False
    for skill_file in sorted(pack_path.rglob("SKILL.md")):
        try:
            text = skill_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if _AUTH_PATTERN.search(text):
            return True
    return False


def _installed_pack_auth_providers(pack) -> tuple[SkillPackAuthProvider, ...]:
    if not _installed_pack_requires_auth(pack):
        return ()
    pack_id = str(getattr(pack, "pack_id", "") or "custom/skill")
    return (
        SkillPackAuthProvider(
            provider_id=_provider_id_for_pack(pack_id),
            name=f"{pack_id} credentials",
            credential_policy="admin_decides",
            notes="Detected auth-related setup language in this custom skill pack.",
        ),
    )


def list_skill_pack_catalog() -> tuple[SkillPackCatalogEntry, ...]:
    return SKILL_PACK_CATALOG


def builtin_nullion_skill_pack_ids() -> tuple[str, ...]:
    """Return the built-in Nullion packs that ship with every install."""
    return tuple(entry.pack_id for entry in SKILL_PACK_CATALOG if entry.source_url == "built-in")


def default_enabled_skill_pack_ids() -> tuple[str, ...]:
    """Skill packs enabled by default; users can disable any of them in setup or Settings."""
    return builtin_nullion_skill_pack_ids()


def get_skill_pack_catalog_entry(pack_id: str) -> SkillPackCatalogEntry | None:
    normalized = pack_id.strip().lower()
    for entry in SKILL_PACK_CATALOG:
        if entry.pack_id == normalized:
            return entry
    for entry in list_available_skill_packs():
        if entry.pack_id == normalized:
            return entry
    return None


def list_available_skill_packs() -> tuple[SkillPackCatalogEntry, ...]:
    """Return built-in catalog entries plus locally installed custom packs."""
    known_ids = {entry.pack_id for entry in SKILL_PACK_CATALOG}
    known_ids.update(BUILTIN_SKILL_PACK_PROMPTS)
    installed_entries = []
    for pack in list_installed_skill_packs():
        if pack.pack_id in known_ids:
            continue
        installed_entries.append(
            SkillPackCatalogEntry(
                pack_id=pack.pack_id,
                name=pack.pack_id,
                source_url=pack.source,
                status="installed",
                summary=f"Installed local skill pack with {pack.skills_count} SKILL.md file(s).",
                coverage=(),
                setup_hint="Enable this pack in Settings or set NULLION_ENABLED_SKILL_PACKS.",
                auth_providers=_installed_pack_auth_providers(pack),
            )
        )
    return SKILL_PACK_CATALOG + tuple(installed_entries)


def list_skill_pack_auth_providers() -> tuple[dict[str, object], ...]:
    providers: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for entry in list_available_skill_packs():
        for provider in entry.auth_providers:
            key = (entry.pack_id, provider.provider_id)
            if key in seen:
                continue
            seen.add(key)
            providers.append({
                "skill_pack_id": entry.pack_id,
                "skill_name": entry.name,
                "provider_id": provider.provider_id,
                "provider_name": provider.name,
                "credential_policy": provider.credential_policy,
                "shared_allowed": provider.shared_allowed,
                "notes": provider.notes,
                "required_tools": list(entry.required_tools),
            })
    try:
        from nullion.connections import load_connection_registry

        for connection in load_connection_registry().connections:
            provider_id = str(getattr(connection, "provider_id", "") or "").strip()
            if not getattr(connection, "active", True) or not _provider_id_looks_external_connector(provider_id):
                continue
            key = ("nullion/connector-skills", provider_id)
            if key in seen:
                continue
            seen.add(key)
            providers.append({
                "skill_pack_id": "nullion/connector-skills",
                "skill_name": "Connector/API Skills",
                "provider_id": provider_id,
                "provider_name": str(getattr(connection, "display_name", "") or provider_id),
                "credential_policy": "admin_decides",
                "shared_allowed": True,
                "notes": "Active connector provider discovered from workspace connections.",
                "required_tools": ["connector_request"],
            })
    except Exception:
        pass
    return tuple(providers)


def skill_pack_access_prompt(
    enabled_pack_ids: tuple[str, ...] | list[str],
    *,
    principal_id: str | None = None,
) -> str:
    enabled = {str(pack_id).strip().lower() for pack_id in enabled_pack_ids if str(pack_id).strip()}
    if not enabled:
        return ""
    entries = [entry for entry in list_available_skill_packs() if entry.pack_id.strip().lower() in enabled]
    if not entries:
        return ""
    try:
        from nullion.connections import connection_for_principal
    except Exception:
        connection_for_principal = None
    lines = [
        "Skill access policy:",
        "- Skill packs provide instructions only; account/API access requires matching tools and an allowed credential policy.",
        "- Use a skill for account data only when its provider connection is available for this workspace or explicitly shared by admin.",
        "- If a native account tool fails because its provider is missing or unauthorized, check enabled connector skills and active connector providers before telling the user the task cannot be done.",
    ]
    for entry in entries:
        if not entry.requires_auth:
            lines.append(f"- {entry.pack_id}: reference-only skill; available to all workspaces.")
            continue
        provider_states: list[str] = []
        provider_ids = {provider.provider_id for provider in entry.auth_providers}
        for provider in entry.auth_providers:
            connected = False
            if connection_for_principal is not None:
                try:
                    connected = connection_for_principal(principal_id, provider.provider_id) is not None
                except Exception:
                    connected = False
            state = "connected" if connected else "missing credential"
            provider_states.append(f"{provider.name} ({provider.provider_id}: {state})")
        if entry.pack_id == "nullion/connector-skills":
            try:
                from nullion.connections import load_connection_registry

                for connection in load_connection_registry().connections:
                    provider_id = str(getattr(connection, "provider_id", "") or "").strip()
                    if provider_id in provider_ids or not getattr(connection, "active", True):
                        continue
                    if not _provider_id_looks_external_connector(provider_id):
                        continue
                    connected = False
                    if connection_for_principal is not None:
                        try:
                            connected = connection_for_principal(principal_id, provider_id) is not None
                        except Exception:
                            connected = False
                    if connected:
                        provider_states.append(
                            f"{getattr(connection, 'display_name', provider_id)} ({provider_id}: connected)"
                        )
            except Exception:
                pass
        tools = ", ".join(entry.required_tools) if entry.required_tools else "provider-backed tools"
        lines.append(f"- {entry.pack_id}: requires auth and {tools}; " + "; ".join(provider_states) + ".")
    return "\n".join(lines)


__all__ = [
    "SKILL_PACK_CATALOG",
    "SkillPackAuthProvider",
    "SkillPackCatalogEntry",
    "builtin_nullion_skill_pack_ids",
    "default_enabled_skill_pack_ids",
    "get_skill_pack_catalog_entry",
    "list_skill_pack_auth_providers",
    "list_available_skill_packs",
    "list_skill_pack_catalog",
    "skill_pack_access_prompt",
]

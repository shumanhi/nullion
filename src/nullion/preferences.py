"""User preferences — formatting, tone, and behaviour hints injected as system context.

Saved to ~/.nullion/preferences.json.
Use load_preferences() to read and build_preferences_prompt() to get the
system-prompt snippet that should be prepended to every AI turn.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, tzinfo
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_PREFS_PATH = Path.home() / ".nullion" / "preferences.json"
_PROFILE_PATH = Path.home() / ".nullion" / "profile.json"

_MAX_PERSONA_LEN = 280  # Twitter-length cap


def _validated_timezone_name(value: object) -> str | None:
    name = str(value or "").strip()
    if not name or name.startswith(":"):
        return None
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None
    return name


def _timezone_from_localtime_link(path: Path) -> str | None:
    try:
        if not path.exists() or not path.is_symlink():
            return None
        resolved = str(path.resolve())
    except OSError:
        return None
    for marker in ("/zoneinfo/", "/zoneinfo.default/"):
        if marker not in resolved:
            continue
        candidate = resolved.split(marker, 1)[1]
        return _validated_timezone_name(candidate)
    return None


def detect_system_timezone(default: str = "UTC") -> str:
    """Best-effort IANA timezone detection for the local machine."""
    for env_name in ("NULLION_TIMEZONE", "TZ"):
        candidate = _validated_timezone_name(os.environ.get(env_name))
        if candidate:
            return candidate

    for localtime_path in (Path("/etc/localtime"), Path("/var/db/timezone/localtime")):
        candidate = _timezone_from_localtime_link(localtime_path)
        if candidate:
            return candidate

    try:
        timezone_file = Path("/etc/timezone")
        if timezone_file.exists():
            candidate = _validated_timezone_name(timezone_file.read_text(encoding="utf-8").splitlines()[0])
            if candidate:
                return candidate
    except (IndexError, OSError):
        pass

    local_tz = datetime.now().astimezone().tzinfo
    key = getattr(local_tz, "key", None)
    return _validated_timezone_name(key) or default


def resolve_timezone(timezone_name: str | None = None) -> tzinfo:
    """Return a tzinfo for saved preferences, falling back to UTC when invalid."""
    name = _validated_timezone_name(timezone_name) or "UTC"
    return ZoneInfo(name) if name != "UTC" else UTC

_DEFAULTS: dict = {
    # Personality
    "persona":             "",           # user-written prompt fragment, max 280 chars
    # Formatting
    "emoji_level":         "standard",   # none | minimal | standard | expressive
    "response_length":     "balanced",   # concise | balanced | detailed
    "response_structure":  "free",       # free | bullets | numbered | prose
    "markdown_style":      "light",      # plain | light | full
    # Tone & voice
    "tone":                "friendly",   # formal | professional | casual | friendly
    "language_complexity": "standard",   # simple | standard | technical
    # Behaviour
    "proactive_suggestions": True,       # suggest related things unprompted
    "auto_mode":              True,      # let low-risk safe tasks continue without prompting
    "approval_strictness":    "balanced",# strict | balanced | relaxed
    "confirm_before_action": False,      # always ask before doing anything
    "show_reasoning":        False,      # explain decisions with brief rationale
    "code_examples":         "relevant", # always | relevant | never
    # Sentinel
    "sentinel_mode":         "risk_based", # allow_all | risk_based | ask_all
    "sentinel_risk_level":   4,            # 3-10; risk_based asks at or above this score
    "outbound_request_mode": "risk_based", # allow_all | risk_based | ask_all
    # Time & locale
    "timezone":    detect_system_timezone(),
    "date_format": "YYYY-MM-DD",         # MM/DD/YYYY | DD/MM/YYYY | YYYY-MM-DD
    "time_format": "12h",                # 12h | 24h
}


@dataclass
class Preferences:
    persona:               str  = ""
    emoji_level:           str  = "standard"
    response_length:       str  = "balanced"
    response_structure:    str  = "free"
    markdown_style:        str  = "light"
    tone:                  str  = "friendly"
    language_complexity:   str  = "standard"
    proactive_suggestions: bool = True
    auto_mode:              bool = True
    approval_strictness:    str  = "balanced"
    confirm_before_action: bool = False
    show_reasoning:        bool = False
    code_examples:         str  = "relevant"
    sentinel_mode:         str  = "risk_based"
    sentinel_risk_level:   int  = 4
    outbound_request_mode: str  = "risk_based"
    timezone:              str  = field(default_factory=detect_system_timezone)
    date_format:           str  = "YYYY-MM-DD"
    time_format:           str  = "12h"

    def to_dict(self) -> dict:
        return {
            "persona":               self.persona,
            "emoji_level":           self.emoji_level,
            "response_length":       self.response_length,
            "response_structure":    self.response_structure,
            "markdown_style":        self.markdown_style,
            "tone":                  self.tone,
            "language_complexity":   self.language_complexity,
            "proactive_suggestions": self.proactive_suggestions,
            "auto_mode":              self.auto_mode,
            "approval_strictness":    self.approval_strictness,
            "confirm_before_action": self.confirm_before_action,
            "show_reasoning":        self.show_reasoning,
            "code_examples":         self.code_examples,
            "sentinel_mode":         self.sentinel_mode,
            "sentinel_risk_level":   self.sentinel_risk_level,
            "outbound_request_mode": self.outbound_request_mode,
            "timezone":              self.timezone,
            "date_format":           self.date_format,
            "time_format":           self.time_format,
        }


def load_preferences() -> Preferences:
    """Load preferences from disk, filling in defaults for missing keys."""
    data: dict = dict(_DEFAULTS)
    if _PREFS_PATH.exists():
        try:
            data.update(json.loads(_PREFS_PATH.read_text()))
        except Exception:
            pass

    raw_persona = str(data.get("persona", "")).strip()
    try:
        sentinel_risk_level = int(data.get("sentinel_risk_level", _DEFAULTS["sentinel_risk_level"]))
    except (TypeError, ValueError):
        sentinel_risk_level = int(_DEFAULTS["sentinel_risk_level"])
    sentinel_risk_level = min(10, max(3, sentinel_risk_level))

    sentinel_mode = str(data.get("sentinel_mode", _DEFAULTS["sentinel_mode"]))
    if sentinel_mode not in {"allow_all", "risk_based", "ask_all"}:
        sentinel_mode = str(_DEFAULTS["sentinel_mode"])
    outbound_request_mode = str(data.get("outbound_request_mode", _DEFAULTS["outbound_request_mode"]))
    if outbound_request_mode not in {"allow_all", "risk_based", "ask_all"}:
        outbound_request_mode = str(_DEFAULTS["outbound_request_mode"])
    approval_strictness = str(data.get("approval_strictness", _DEFAULTS["approval_strictness"]))
    if approval_strictness not in {"strict", "balanced", "relaxed"}:
        approval_strictness = str(_DEFAULTS["approval_strictness"])

    return Preferences(
        persona               = raw_persona[:_MAX_PERSONA_LEN],
        emoji_level           = str(data.get("emoji_level",           _DEFAULTS["emoji_level"])),
        response_length       = str(data.get("response_length",       _DEFAULTS["response_length"])),
        response_structure    = str(data.get("response_structure",    _DEFAULTS["response_structure"])),
        markdown_style        = str(data.get("markdown_style",        _DEFAULTS["markdown_style"])),
        tone                  = str(data.get("tone",                  _DEFAULTS["tone"])),
        language_complexity   = str(data.get("language_complexity",   _DEFAULTS["language_complexity"])),
        proactive_suggestions = bool(data.get("proactive_suggestions",_DEFAULTS["proactive_suggestions"])),
        auto_mode             = bool(data.get("auto_mode",             _DEFAULTS["auto_mode"])),
        approval_strictness   = approval_strictness,
        confirm_before_action = bool(data.get("confirm_before_action",_DEFAULTS["confirm_before_action"])),
        show_reasoning        = bool(data.get("show_reasoning",       _DEFAULTS["show_reasoning"])),
        code_examples         = str(data.get("code_examples",         _DEFAULTS["code_examples"])),
        sentinel_mode         = sentinel_mode,
        sentinel_risk_level   = sentinel_risk_level,
        outbound_request_mode = outbound_request_mode,
        timezone              = str(data.get("timezone",              _DEFAULTS["timezone"])),
        date_format           = str(data.get("date_format",           _DEFAULTS["date_format"])),
        time_format           = str(data.get("time_format",           _DEFAULTS["time_format"])),
    )


def save_preferences(prefs: Preferences | dict) -> None:
    _PREFS_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = prefs.to_dict() if isinstance(prefs, Preferences) else prefs
    _PREFS_PATH.write_text(json.dumps(data, indent=2) + "\n")


# ── Prompt builder ────────────────────────────────────────────────────────────

_EMOJI_LINES = {
    "none":       "Use no emojis whatsoever in your replies.",
    "minimal":    "Use emojis very sparingly — only for the most important moments (errors, success, warnings). Maximum 1–2 per reply.",
    "standard":   "Use emojis in moderation where they add clarity or warmth.",
    "expressive": "Feel free to use emojis liberally to make replies lively and expressive.",
}

_LENGTH_LINES = {
    "concise":  "Keep responses brief and to the point. Avoid filler sentences.",
    "balanced": "Aim for a natural response length — not too short, not too long.",
    "detailed": "Provide thorough, comprehensive responses. Include context, examples, and edge cases.",
}

_STRUCTURE_LINES = {
    "free":     "Choose the most natural structure for each reply.",
    "bullets":  "Prefer bullet-point lists to organise information.",
    "numbered": "Use numbered lists when presenting steps, options, or ranked items.",
    "prose":    "Write in flowing paragraphs — avoid bullet points and headers unless absolutely necessary.",
}

_MARKDOWN_LINES = {
    "plain": "Do not use any markdown formatting — plain text only.",
    "light": "Use light markdown: **bold** for key terms, `code` for commands, but avoid heavy headers.",
    "full":  "Use full markdown with headers (##), bold, italics, tables, and code blocks as appropriate.",
}

_TONE_LINES = {
    "formal":       "Maintain a formal, professional tone — avoid contractions and casual language.",
    "professional": "Use a professional, polished tone while staying approachable.",
    "casual":       "Use a relaxed, conversational tone — contractions, informal phrasing are fine.",
    "friendly":     "Be warm, friendly, and personable. Like a knowledgeable friend, not a formal assistant.",
}

_COMPLEXITY_LINES = {
    "simple":   "Use simple, plain language. Avoid jargon and technical terms unless the user introduces them.",
    "standard": "Use clear, everyday language. Explain technical terms briefly when they're necessary.",
    "technical":"Use precise technical language — the user is comfortable with domain-specific terminology.",
}

_CODE_LINES = {
    "always":   "Always include code examples and command samples, even for conceptual questions.",
    "relevant": "Include code examples when they genuinely help illustrate the answer.",
    "never":    "Avoid code examples unless the user explicitly asks for code.",
}


def _format_current_time_hint(prefs: Preferences, *, now: datetime | None = None) -> str:
    current_utc = now or datetime.now(UTC)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=UTC)
    local_now = current_utc.astimezone(resolve_timezone(prefs.timezone))
    return (
        f"Current local date and time: {local_now.isoformat(timespec='seconds')} "
        f"({prefs.timezone}). Current UTC time: {current_utc.astimezone(UTC).isoformat(timespec='seconds')}. "
        "For reminders and other time-sensitive tasks, interpret relative times from the current local time; "
        "when a relative reminder delay is requested, prefer the set_reminder due_in_seconds argument."
    )


def build_preferences_prompt(prefs: Preferences, *, now: datetime | None = None) -> str:
    """Build a concise system-prompt snippet from the user's preferences."""
    lines: list[str] = []

    # Persona comes first — it shapes everything else
    if prefs.persona.strip():
        lines.append("## Your personality / how to behave:")
        lines.append(prefs.persona.strip())
        lines.append("")

    lines.append("## Formatting and tone rules:")

    lines.append(_EMOJI_LINES.get(prefs.emoji_level, _EMOJI_LINES["standard"]))
    lines.append(_LENGTH_LINES.get(prefs.response_length, _LENGTH_LINES["balanced"]))
    lines.append(_STRUCTURE_LINES.get(prefs.response_structure, _STRUCTURE_LINES["free"]))
    lines.append(_MARKDOWN_LINES.get(prefs.markdown_style, _MARKDOWN_LINES["light"]))
    lines.append(_TONE_LINES.get(prefs.tone, _TONE_LINES["friendly"]))
    lines.append(_COMPLEXITY_LINES.get(prefs.language_complexity, _COMPLEXITY_LINES["standard"]))
    lines.append(_CODE_LINES.get(prefs.code_examples, _CODE_LINES["relevant"]))

    if prefs.proactive_suggestions:
        lines.append("When relevant, proactively suggest related ideas, follow-ups, or improvements the user hasn't asked for.")
    else:
        lines.append("Only answer what was asked. Do not add unsolicited suggestions or tangents.")

    if prefs.confirm_before_action:
        lines.append("Always confirm with the user before taking any irreversible action.")

    if prefs.show_reasoning:
        lines.append("Include a brief rationale, assumptions, and tradeoffs when useful, without exposing hidden chain-of-thought.")

    tz_hint = f"The user's timezone is {prefs.timezone}." if prefs.timezone else ""
    date_hint = f"Format dates as {prefs.date_format}."
    time_hint = f"Use {prefs.time_format} time format."
    lines.append(" ".join(filter(None, [tz_hint, date_hint, time_hint])))
    lines.append(_format_current_time_hint(prefs, now=now))

    return "\n".join(lines)


def load_profile() -> dict[str, str]:
    """Load the local operator profile used to personalize all chat surfaces."""
    if not _PROFILE_PATH.exists():
        return {}
    try:
        loaded = json.loads(_PROFILE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(loaded, dict):
        return {}
    allowed = {"name", "email", "phone", "address", "notes"}
    return {
        key: str(value).strip()
        for key, value in loaded.items()
        if key in allowed and isinstance(value, str) and value.strip()
    }


def build_profile_prompt(profile: dict[str, str] | None = None) -> str | None:
    """Build the shared profile prompt for web, Telegram, and future chat apps."""
    data = load_profile() if profile is None else profile
    parts = [f"{key.title()}: {value}" for key, value in data.items() if isinstance(value, str) and value.strip()]
    if not parts:
        return None
    return "User profile:\n" + "\n".join(parts)

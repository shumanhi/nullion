"""Shared formatting for user-facing tips."""

from __future__ import annotations

TIP_EMOJI = "💡"
SETUP_TIP_LABEL = "Setup tip"
WEB_RESEARCH_SETUP_TIP = (
    "adding a Brave Search API key in Settings would make weather/web research faster and more reliable."
)
IMAGE_GENERATION_SETUP_TIP = "configure an image generation provider in Settings for better generated images."
MEDIA_PROVIDER_SETUP_TIP = (
    "enable media_plugin with a local provider binding, then configure the matching media provider in Settings "
    "or set NULLION_AUDIO_TRANSCRIBE_COMMAND, NULLION_IMAGE_OCR_COMMAND, or NULLION_IMAGE_GENERATE_COMMAND as needed."
)


def format_tip(text: str, *, label: str = "Tip") -> str:
    normalized_label = str(label or "Tip").strip().rstrip(":") or "Tip"
    body = str(text or "").strip()
    return f"{TIP_EMOJI} {normalized_label}: {body}" if body else f"{TIP_EMOJI} {normalized_label}:"


def format_setup_tip(text: str) -> str:
    return format_tip(text, label=SETUP_TIP_LABEL)


def setup_tip_instruction(text: str) -> str:
    return f"include one short setup tip in this format: {format_setup_tip(text)}"


__all__ = [
    "IMAGE_GENERATION_SETUP_TIP",
    "MEDIA_PROVIDER_SETUP_TIP",
    "SETUP_TIP_LABEL",
    "TIP_EMOJI",
    "WEB_RESEARCH_SETUP_TIP",
    "format_setup_tip",
    "format_tip",
    "setup_tip_instruction",
]

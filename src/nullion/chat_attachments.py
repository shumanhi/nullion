"""Shared chat attachment handling for web and messaging adapters."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import mimetypes
from pathlib import Path
from typing import Any


IMAGE_MEDIA_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
AUDIO_MEDIA_TYPES = {
    "audio/aac",
    "audio/flac",
    "audio/m4a",
    "audio/mp4",
    "audio/mpeg",
    "audio/ogg",
    "audio/wav",
    "audio/webm",
    "audio/x-m4a",
    "audio/x-wav",
}
AUDIO_EXTENSIONS = {".aac", ".flac", ".m4a", ".mp3", ".oga", ".ogg", ".opus", ".wav", ".weba", ".webm"}
VIDEO_MEDIA_TYPES = {
    "video/mp4",
    "video/mpeg",
    "video/quicktime",
    "video/webm",
    "video/x-m4v",
    "video/x-matroska",
    "video/x-msvideo",
}
VIDEO_EXTENSIONS = {".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".webm"}
MAX_INLINE_IMAGE_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ChatAttachment:
    name: str
    path: str
    media_type: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "path": self.path, "media_type": self.media_type}


def guess_media_type(path_or_name: str, fallback: str = "application/octet-stream") -> str:
    media_type, _ = mimetypes.guess_type(path_or_name)
    return media_type or fallback


def is_supported_image_attachment(attachment: ChatAttachment) -> bool:
    media_type = attachment.media_type.lower()
    suffix = Path(attachment.name or attachment.path).suffix.lower()
    return media_type in IMAGE_MEDIA_TYPES or suffix in IMAGE_EXTENSIONS


def is_supported_audio_attachment(attachment: ChatAttachment) -> bool:
    media_type = attachment.media_type.lower()
    suffix = Path(attachment.name or attachment.path).suffix.lower()
    return media_type in AUDIO_MEDIA_TYPES or media_type.startswith("audio/") or suffix in AUDIO_EXTENSIONS


def is_supported_video_attachment(attachment: ChatAttachment) -> bool:
    media_type = attachment.media_type.lower()
    suffix = Path(attachment.name or attachment.path).suffix.lower()
    return media_type in VIDEO_MEDIA_TYPES or media_type.startswith("video/") or suffix in VIDEO_EXTENSIONS


def _normalized_tool_status(result: object) -> str:
    status = getattr(result, "status", "")
    value = getattr(status, "value", status)
    return str(value or "").strip().lower()


def _tool_output_text(result: object) -> str:
    output = getattr(result, "output", None)
    if isinstance(output, dict):
        for key in ("text", "transcript", "summary"):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if isinstance(output, str) and output.strip():
        return output.strip()
    return ""


def audio_transcription_satisfied(tool_results: list[object] | tuple[object, ...]) -> bool:
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "") != "audio_transcribe":
            continue
        if _normalized_tool_status(result) not in {"completed", "approved"}:
            continue
        if _tool_output_text(result):
            return True
    return False


def attachment_processing_failure_reply(
    message: str,
    attachments: list[ChatAttachment],
    tool_results: list[object] | tuple[object, ...],
) -> str | None:
    """Return a user-visible failure when an uploaded attachment contract was not met."""
    if not any(is_supported_audio_attachment(attachment) for attachment in attachments):
        return None
    if audio_transcription_satisfied(tool_results):
        return None
    return (
        "I couldn't transcribe the attached audio file. "
        "I won't mark this complete until audio transcription succeeds."
    )


def is_supported_chat_file(*, filename: str | None, media_type: str | None = None) -> bool:
    """Return whether a platform-supplied file should enter the shared attachment pipeline."""
    safe_name = Path(str(filename or "").strip()).name
    if safe_name and safe_name not in {".", ".."}:
        return True
    clean_media_type = str(media_type or "").strip()
    return bool(clean_media_type)


def normalize_chat_attachments(value: object) -> list[ChatAttachment]:
    if not isinstance(value, list):
        return []
    attachments: list[ChatAttachment] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        raw_path = str(item.get("path") or "").strip()
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        if not path.exists() or not path.is_file():
            continue
        name = Path(str(item.get("name") or path.name)).name or path.name
        media_type = str(item.get("media_type") or guess_media_type(name)).strip() or "application/octet-stream"
        attachments.append(ChatAttachment(name=name, path=str(path), media_type=media_type))
    return attachments


def chat_attachment_content_blocks(message: str, attachments: list[ChatAttachment]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    if message:
        blocks.append({"type": "text", "text": message})
    for attachment in attachments:
        path = Path(attachment.path)
        descriptor = f"\n\nAttached file: {attachment.name} ({attachment.media_type}) at {attachment.path}"
        if is_supported_image_attachment(attachment):
            try:
                if path.stat().st_size > MAX_INLINE_IMAGE_BYTES:
                    blocks.append({"type": "text", "text": descriptor + "\nImage is too large to inline for vision."})
                    continue
                data = base64.b64encode(path.read_bytes()).decode("ascii")
            except OSError:
                blocks.append({"type": "text", "text": descriptor + "\nImage could not be read."})
                continue
            blocks.append({"type": "text", "text": descriptor})
            blocks.append(
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": attachment.media_type,
                        "data": data,
                    },
                }
            )
        else:
            if is_supported_audio_attachment(attachment):
                descriptor += (
                    "\nUse audio_transcribe with this exact path when the user asks what is in the audio. "
                    "Do not invent, convert to, or request alternate paths under /tmp, /var/tmp, or /var/folders."
                )
            elif is_supported_video_attachment(attachment):
                descriptor += (
                    "\nThis is a video attachment. Use the configured video-capable model/provider for analysis "
                    "when available, and refer to this exact path. Do not invent, convert to, or request alternate "
                    "paths under /tmp, /var/tmp, or /var/folders."
                )
            else:
                descriptor += (
                    "\nThis file is already uploaded and available at the exact local path above. "
                    "Use the file/document tools available in this runtime to inspect or modify it when needed. "
                    "Do not ask the user to upload it again."
                )
            blocks.append({"type": "text", "text": descriptor})
    if not blocks:
        blocks.append({"type": "text", "text": message})
    return blocks


__all__ = [
    "ChatAttachment",
    "attachment_processing_failure_reply",
    "audio_transcription_satisfied",
    "chat_attachment_content_blocks",
    "guess_media_type",
    "is_supported_audio_attachment",
    "is_supported_chat_file",
    "is_supported_image_attachment",
    "is_supported_video_attachment",
    "normalize_chat_attachments",
    "VIDEO_EXTENSIONS",
]

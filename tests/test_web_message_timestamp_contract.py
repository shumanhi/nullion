from __future__ import annotations

import os
from pathlib import Path


APP_ROOT = Path(os.environ.get("NULLION_APP_REPO", Path(__file__).resolve().parents[1])).resolve()


def _web_source() -> str:
    return (APP_ROOT / "src" / "nullion" / "web_app.py").read_text(encoding="utf-8")


def test_web_chat_messages_render_lightweight_timestamps() -> None:
    source = _web_source()

    assert ".message-time" in source
    assert "function ensureBubbleTimestamp" in source
    assert "messageTimestampFromMetadata" in source
    assert "created_at: m.created_at || ''" in source
    assert "ensureBubbleTimestamp(bubble);" in source

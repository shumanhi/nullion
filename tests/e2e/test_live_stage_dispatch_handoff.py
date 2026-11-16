"""Small, real-provider Web replay; no email transport or real job mutations."""
import json
import os
from pathlib import Path
import time

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, _turns

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_LIVE_STAGE_E2E") != "1", reason="Opt-in live stage quota")


def test_live_vague_followup_does_not_cancel_generated_file(live_web):
    from playwright.sync_api import expect
    page, frames, errors, database, conversation, base = live_web
    _submit(live_web, "For this chat, a separate movie-invitation discussion has these details: 8 PM, my place, takeout. Acknowledge only; do not send anything or save memory.", timeout=90)
    destination = Path(os.environ["NULLION_LIVE_STAGE_ARTIFACT_ROOT"]) / "flight-status-fixture.png"
    prompt = ("Create and attach a readable 640 by 480 PNG diagram of a fictional flight-status dashboard. "
        "Use headings Departures and Arrivals with example values 12 and 9. This is a local rendering fixture; "
        "do not browse or obtain live data. Use the local terminal/Pillow fallback if image generation is "
        f"unavailable. Declare terminal_exec output_paths as [{json.dumps(str(destination))}] and create that file.")
    before = len(frames)
    page.locator("#user-input").fill(prompt)
    page.locator("#user-input").press("Enter")
    # HTTP fallback has no intermediate server frames. Read the real composer
    # turn state rather than requiring a WebSocket-only admission event.
    page.wait_for_function("() => _activeSendTurnIds.size > 0", timeout=10000)
    first_id = page.evaluate("Array.from(_activeSendTurnIds).at(-1)")
    assert first_id
    page.wait_for_timeout(250)
    assert not any(frame.get("type") == "done" and frame.get("turn_id") == first_id for frame in frames[before:]), "Producer completed before concurrent replay"
    page.locator("#user-input").fill("Fix it")
    page.locator("#user-input").press("Enter")
    expect(page.locator(".msg.bot .bubble").filter(has_text="Which request is this message for?")).to_be_visible(timeout=90000)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        terminal = next((frame for frame in frames[before:] if frame.get("type") == "done"
            and frame.get("turn_id") == first_id), None)
        if terminal: break
        page.wait_for_timeout(100)
    assert terminal and not terminal.get("superseded"), frames[before:]
    artifacts = terminal.get("artifacts") or []
    assert len(artifacts) == 1 and artifacts[0]["name"].endswith(".png"), terminal
    content = page.request.get(base + artifacts[0]["url"]).body()
    assert content.startswith(b"\x89PNG\r\n\x1a\n") and len(content) > 500
    turns = _turns(database, conversation)
    original = next(row for row in turns if row.get("turn_id") == first_id)
    question = next(row for row in turns if row.get("user_message") == "Fix it")
    assert question.get("parent_turn_id") is None
    assert any(result.get("tool_name") == "terminal_exec" and result.get("status") == "completed"
        for result in original.get("tool_results") or [])
    assert not errors

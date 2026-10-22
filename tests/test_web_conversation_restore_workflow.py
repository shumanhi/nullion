from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from zoneinfo import ZoneInfo


@pytest.mark.parametrize("reset_action", ["archive", "clear"])
def test_latest_web_conversation_stays_blank_after_reset_action(tmp_path, monkeypatch, reset_action: str) -> None:
    import nullion.chat_store as chat_store_module
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import _remember_web_chat_turn, create_app

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    chat_store_module._store = None
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    previous_id = "web:previous"
    current_id = "web:current"
    fresh_id = "web:fresh"
    for conversation_id, text in ((previous_id, "older thread"), (current_id, "thread to reset")):
        response = client.post(
            f"/api/chat/history/{conversation_id}/message",
            json={"role": "user", "text": text},
        )
        assert response.status_code == 200

    assert client.get("/api/chat/conversations/latest?channel=web").json()["conversation"]["id"] == current_id

    if reset_action == "archive":
        reset_response = client.post(f"/api/chat/history/{current_id}/archive")
    else:
        reset_response = client.delete(f"/api/chat/history/{current_id}")
    assert reset_response.status_code == 200

    latest_after_reset = client.get("/api/chat/conversations/latest?channel=web").json()
    assert latest_after_reset == {"ok": True, "conversation": None}

    fresh_response = client.post(
        f"/api/chat/history/{fresh_id}/message",
        json={"role": "user", "text": "new thread"},
    )
    assert fresh_response.status_code == 200
    _remember_web_chat_turn(runtime, conversation_id=fresh_id, user_message="new thread", assistant_reply="reply")
    assert client.get("/api/chat/conversations/latest?channel=web").json()["conversation"]["id"] == fresh_id


def test_chat_history_endpoint_filters_messages_by_user_local_date(tmp_path, monkeypatch) -> None:
    import nullion.chat_store as chat_store_module
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    monkeypatch.setattr(chat_store_module, "_local_history_timezone", lambda: ZoneInfo("America/New_York"))
    chat_store_module._store = None
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)
    store = chat_store_module.get_chat_store()
    store.import_runtime_chat_turns(
        [
            {
                "conversation_id": "web:local-evening",
                "created_at": "2026-05-16T01:30:00+00:00",
                "user_message": "late local request",
                "assistant_reply": "late local reply",
            }
        ]
    )
    store.import_runtime_chat_turns(
        [
            {
                "conversation_id": "web:local-evening",
                "created_at": f"2026-05-17T01:{index % 60:02d}:00+00:00",
                "user_message": f"next local day request {index}",
                "assistant_reply": f"next local day reply {index}",
            }
            for index in range(350)
        ]
    )

    local_day = client.get("/api/chat/history/web:local-evening?date=2026-05-15")
    next_local_day = client.get("/api/chat/history/web:local-evening?date=2026-05-16")

    assert local_day.status_code == 200
    assert [message["text"] for message in local_day.json()["messages"]] == [
        "late local request",
        "late local reply",
    ]
    assert next_local_day.status_code == 200
    assert len(next_local_day.json()["messages"]) == 700

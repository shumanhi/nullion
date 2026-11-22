from nullion.conversation_history_tools import _recent_browser_snapshot, compact_history_search_output
from nullion.plugins.browser_plugin.browser_use_adapter import _browser_decision_page_view
from nullion.source_observation_contract import compact_source_observations


def capture(url, timestamp, price):
    return {
        "result_kind": "source_observations", "report_status": "completed", "page_url": url,
        "captured_documents": [{"source_url": url, "captured_at": timestamp, "visible_text_verified": True, "main_document_text": price + "\nOtra opción $35"}],
        "observations": [{"label": "Precio", "value": price, "quote": price, "semantic_role": "entity",
                          "source_url": url, "captured_at": timestamp, "evidence_path": "/internal/capture.png"}],
    }


def test_snapshot_age_workspace_and_exact_evidence():
    url = "https://example.com/results"
    raw = capture(url, 1000, "$20")
    raw["captured_documents"] += [{"source_url": url, "captured_at": 1001, "main_document_text": "Unverified state"}]
    output = compact_source_observations(raw, retain_result_page=True)
    assert "retained_result_page" not in compact_source_observations(raw)
    event = {"conversation_id": "web:one", "workspace_id": "workspace_one", "turn_id": "older",
             "tool_results": [{"tool_name": "browser_run_task", "status": "completed", "output": output,
                               "invocation_id": "original"}]}

    class Store:
        events = [event]

        def list_recent_conversation_events_after_reset(self, conversation_id, *, event_type, limit):
            assert (conversation_id, event_type, limit) == ("web:one", "conversation.chat_turn", 8)
            return self.events

    store = Store()
    snapshot = _recent_browser_snapshot(store, "web:one", "workspace_one", 60, now=1050)
    assert snapshot["source_invocation_id"] == "original"
    assert snapshot["page"]["text"] == "$20\nOtra opción $35"
    assert "evidence_path" not in snapshot["observations"][0]
    assert _recent_browser_snapshot(store, "web:one", "workspace_one", 49, now=1050)["reason"] == "no_fresh_verified_browser_evidence"
    assert _recent_browser_snapshot(store, "web:one", "workspace_other", 60, now=1050)["reason"] == "no_recent_browser_lookup"
    legacy_output = compact_source_observations(raw)
    legacy_output["sources"][0]["main_document_text"] = "$20\nOtra opción $35"
    legacy_event = {**event, "tool_results": [{**event["tool_results"][0], "output": legacy_output}]}
    store.events = [legacy_event]
    assert _recent_browser_snapshot(store, "web:one", "workspace_one", 60, now=1050)["page"] == snapshot["page"]
    store.events.append({**event, "tool_results": [{"tool_name": "browser_extract_text", "status": "failed",
                                                  "output": {"reason": "source_not_ready"}}]})
    assert _recent_browser_snapshot(store, "web:one", "workspace_one", 60, now=1050)["reason"] == "latest_browser_lookup_incomplete"
    store.events.pop()
    store.events.append({**event, "tool_results": [{"tool_name": "browser_run_task", "status": "failed"}]})
    assert _recent_browser_snapshot(store, "web:one", "workspace_one", 60, now=1050)["reason"] == "latest_browser_lookup_incomplete"
    compact = compact_history_search_output({"result_kind": "retained_browser_snapshot", **snapshot}, max_chars=210)
    assert "page" not in compact and compact["page_omitted"]


def test_browser_decision_preview_preserves_exact_references_and_selected_option():
    options = [{"value": str(i), "selected": i == 2} for i in range(200)]
    lines = [{"index": i + 5, "text": "電車" * 100} for i in range(100)]
    state = {"source_id": 9, "source_url": "https://example.com", "text_lines": lines,
             "next_start_line": 105, "visible_text": "duplicate",
             "controls": [{"tag": "SELECT", "control_index": 7, "options": options, "text": "long catalog"}]}
    view = _browser_decision_page_view(state)
    assert view["source_id"] == 9 and view["text_lines"] == lines[:len(view["text_lines"])]
    assert view["next_start_line"] == 5 + len(view["text_lines"])
    assert "visible_text" not in view
    assert view["controls"] == [{"tag": "SELECT", "control_index": 7, "option_count": 200,
                                "selected_options": [{"value": "2", "selected": True}]}]
    assert state["controls"][0]["options"] == options

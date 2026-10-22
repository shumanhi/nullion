from nullion.chat_operator import _recent_tool_context_prompt, _recent_tool_scopes_for_context
from nullion.runtime import bootstrap_persistent_runtime


def test_followup_tool_evidence_uses_resolved_parent_not_old_conversation_work(tmp_path):
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    cid = "web:lineage"
    runtime.process_conversation_message(conversation_id=cid, user_message="old work", turn_id="old")
    runtime.store.add_conversation_event({
        "event_type": "conversation.chat_turn", "conversation_id": cid, "turn_id": "old",
        "tool_results": [{"tool_name": "run_cron", "status": "completed", "output": {"id": "old-job"}}],
    })
    runtime.process_conversation_message(conversation_id=cid, user_message="new topic", turn_id="new",
                                         dispatch_disposition="independent", dispatch_reason="new_request")
    runtime.store.add_conversation_event({
        "event_type": "conversation.chat_turn", "conversation_id": cid, "turn_id": "new", "tool_results": [],
    })
    assert _recent_tool_scopes_for_context(runtime, cid, anchor_turn_id="new") == ()
    assert _recent_tool_context_prompt(runtime, cid, anchor_turn_id="new") is None
    assert _recent_tool_scopes_for_context(runtime, cid, anchor_turn_id="old") == ("scheduler",)
    assert "old-job" in _recent_tool_context_prompt(runtime, cid, anchor_turn_id="old")
    assert _recent_tool_context_prompt(runtime, cid, anchor_turn_id="missing") is None


def test_tool_evidence_keeps_parent_ancestry_and_rejects_other_conversations(tmp_path):
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    cid = "web:lineage"
    runtime.process_conversation_message(conversation_id=cid, user_message="lookup", turn_id="lookup")
    runtime.store.add_conversation_event({
        "event_type": "conversation.chat_turn", "conversation_id": cid, "turn_id": "lookup",
        "tool_results": [{"tool_name": "web_fetch", "status": "completed", "output": {"url": "https://example.test/product"}}],
    })
    runtime.process_conversation_message(conversation_id=cid, user_message="revision", turn_id="revision",
                                         dispatch_disposition="revise", dispatch_dependency_turn_ids=("lookup",),
                                         dispatch_reason="model_structured_revision")
    assert _recent_tool_scopes_for_context(runtime, cid, anchor_turn_id="revision") == ("web",)
    assert "https://example.test/product" in _recent_tool_context_prompt(runtime, cid, anchor_turn_id="revision")
    assert _recent_tool_context_prompt(runtime, "web:other", anchor_turn_id="lookup") is None

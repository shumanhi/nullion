"""Durable target choices for unanchored concurrent follow-ups.

Only a structured dispatch decision can open this menu. Numeric replies are UI
selections, not natural-language intent classification. No task is changed until
the selected conversation/turn has been revalidated.
"""

from dataclasses import replace
from datetime import datetime, timedelta, timezone


CHOICE_EVENT = "conversation.dispatch_choice"


def _events(store, conversation_id, event_type, limit):
    latest = getattr(store, "latest_conversation_event_after_reset", None)
    if limit == 1 and callable(latest):
        row = latest(conversation_id, event_type)
        return [row] if row is not None else []
    read = getattr(store, "list_recent_conversation_events_after_reset", None)
    return read(conversation_id, event_type=event_type, limit=limit) if callable(read) else []


def _eligible(store, conversation_id, turn_id):
    turn = store.get_conversation_turn(turn_id)
    if turn is None or turn.conversation_id != conversation_id:
        return False
    branch = store.get_conversation_branch(turn.branch_id)
    return branch is None or str(getattr(branch.status, "value", branch.status)) not in {"cancelled", "superseded"}


def dispatch_candidates(store, conversation_id, active_ids, active_texts):
    """Include the current conversation alongside genuinely active requests.

    Historical requests are available to recall, but are not all candidates
    merely because they happen to be among the last few messages.
    """
    if store is None or not callable(getattr(store, "get_conversation_turn", None)):
        return active_ids, active_texts

    def family(turn_id):
        seen = set()
        for _ in range(12):
            if turn_id in seen:
                break
            seen.add(turn_id)
            turn = store.get_conversation_turn(turn_id)
            if turn is None or turn.conversation_id != conversation_id or not turn.parent_turn_id:
                break
            turn_id = turn.parent_turn_id
        return turn_id

    ids, texts = list(active_ids), list(active_texts)
    families = {family(identity) for identity in ids}
    for event in reversed(_events(store, conversation_id, "conversation.chat_turn", 8)):
        identity = str(event.get("turn_id") or "")
        if not identity or not _eligible(store, conversation_id, identity):
            continue
        turn = store.get_conversation_turn(identity)
        if turn.disposition_reason == "dispatch_target_clarification":
            continue
        root = family(identity)
        if root in families:
            continue
        families.add(root)
        ids.append(identity)
        root_turn = store.get_conversation_turn(root)
        texts.append(root_turn.user_message if root_turn else str(event.get("user_message") or ""))
        break
    return tuple(ids), tuple(texts)


def dispatch_candidate_contexts(store, conversation_id, ids, active_ids):
    """Bounded, indexed evidence for the existing semantic admission decision."""
    rows = list(reversed(_events(store, conversation_id, "conversation.chat_turn", 8))) if store else []
    contexts = []
    for identity in ids:
        match = None
        for row in rows:
            turn_id, seen = row.get("turn_id"), set()
            for _ in range(12):
                if turn_id == identity:
                    match = row
                    break
                if not turn_id or turn_id in seen:
                    break
                seen.add(turn_id)
                turn = store.get_conversation_turn(turn_id)
                turn_id = turn.parent_turn_id if turn else None
            if match is not None:
                break
        contexts.append({"is_running": identity in active_ids,
            "last_reply": str((match or {}).get("assistant_reply") or "")[:700],
            "completed_tools": [result.get("tool_name") for result in (match or {}).get("tool_results", ())
                                if isinstance(result, dict) and result.get("status") == "completed"][:12]})
    return tuple(contexts)


def selected_conversation_option(store, conversation_id, message):
    from .conversation_runtime import ConversationTurnDisposition
    from .intent_router import has_unambiguous_numbered_selection
    from .turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy
    if store is None:
        return None
    latest = _events(store, conversation_id, "conversation.chat_turn", 1)
    if not latest:
        return None
    question = latest[-1]
    identity = question.get("turn_id")
    if (not identity or not _eligible(store, conversation_id, identity)
            or not has_unambiguous_numbered_selection(message, question.get("assistant_reply"))):
        return None
    return TurnDispatchDecision(policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
        dependency_turn_ids=(identity,), disposition=ConversationTurnDisposition.CONTINUE,
        reason="numbered_conversation_choice")


def record_dispatch_choice(store, conversation_id, question_turn_id, message, decision):
    if store is None:
        return
    store.add_conversation_event({
        "event_type": CHOICE_EVENT, "conversation_id": conversation_id,
        "turn_id": question_turn_id, "status": "pending", "original_message": message,
        "targets": [list(row) for row in decision.clarification_targets],
        "effect": decision.clarification_effect.value,
        "question": dispatch_choice_question(decision),
        "valid_until": (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })


def selected_dispatch_choice(store, conversation_id, message, reply_context=None):
    from .conversation_runtime import ConversationTurnDisposition
    from .turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    if store is None:
        return None
    rows = _events(store, conversation_id, CHOICE_EVENT, 1)
    if not rows or rows[-1].get("status") != "pending":
        return None
    pending = rows[-1]
    try:
        if datetime.fromisoformat(pending["valid_until"]) <= datetime.now(timezone.utc):
            return None
    except (KeyError, ValueError, TypeError):
        return None
    # An explicit reply to another message must never consume this question.
    context = reply_context or {}
    anchored = (str(context.get("reply_to_turn_id") or "") == pending["turn_id"]
        or bool(context.get("reply_to_text") and context["reply_to_text"] == pending.get("question")))
    if context and not anchored:
        return None
    selected = str(message or "").strip()
    if not selected.isdecimal() or len(selected) > 2:
        return None
    targets = tuple(tuple(row) for row in pending["targets"])
    option = int(selected)
    base = TurnDispatchDecision(policy=TurnDispatchPolicy.PARALLEL,
        reason="ambiguous_dispatch_target", clarification_targets=targets,
        clarification_effect=ConversationTurnDisposition(pending.get("effect", "continue")),
        resolved_message=pending["original_message"])
    latest = _events(store, conversation_id, "conversation.chat_turn", 1)
    if not anchored and (not latest or latest[-1].get("turn_id") != pending["turn_id"]):
        # An older concurrent task can finish after this menu was delivered.
        # Its late result is not a new user choice or a new request. Re-show
        # the menu rather than letting the number enter that task's context.
        question = store.get_conversation_turn(pending["turn_id"])
        completed = store.get_conversation_turn(latest[-1].get("turn_id")) if latest else None
        if question is not None and completed is not None and completed.created_at < question.created_at:
            return base
        return None
    if not 1 <= option <= len(targets) + 1:
        return base
    dependencies = () if option == len(targets) + 1 else (targets[option - 1][0],)
    if dependencies and not _eligible(store, conversation_id, dependencies[0]):
        remaining = tuple(row for row in targets if _eligible(store, conversation_id, row[0]))
        return replace(base, clarification_targets=remaining, reason="dispatch_target_unavailable")
    effect = ConversationTurnDisposition(pending.get("effect", "continue"))
    decision = TurnDispatchDecision(
        policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE if dependencies else TurnDispatchPolicy.PARALLEL,
        dependency_turn_ids=dependencies,
        disposition=effect if dependencies else ConversationTurnDisposition.INDEPENDENT,
        reason="numbered_dispatch_choice", resolved_message=pending["original_message"])
    store.add_conversation_event({**pending, "status": "selected", "selected_option": option,
        "created_at": datetime.now(timezone.utc).isoformat()})
    return decision


def dispatch_choice_question(decision):
    if decision.reason not in {"ambiguous_dispatch_target", "dispatch_target_unavailable"}:
        return None
    lines = ["Which request is this message for? Reply with a number:"]
    if decision.reason == "dispatch_target_unavailable":
        lines.insert(0, "That request is no longer available. Please choose another target.")
    for index, (_, text) in enumerate(decision.clarification_targets, 1):
        summary = " ".join(str(text).split())
        lines.append(f"{index}. {summary[:157] + '…' if len(summary) > 160 else summary}")
    lines.append(f"{len(decision.clarification_targets) + 1}. A separate request.")
    return "\n".join(lines)

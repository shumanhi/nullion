"""Numeric replies resolve only durable, delivered tracker continuation choices.

No intent classification or model call is needed for an explicit numbered UI
selection. All other turns keep their existing route.
"""

from nullion.live_tracking import ContinuationWindowEnded, local_time, public_tracker
from nullion.tools import ToolResult, _action_receipt


def is_storage_busy(exc):
    import sqlite3
    code = getattr(exc, "sqlite_errorcode", 0)
    return isinstance(exc, sqlite3.OperationalError) and (code & 0xFF) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}


def _choose_tracker_reply(choices, option, *, anchor_turn_id=None):
    def action(row):
        if row.get("choice_kind") == "matched":
            return {1: "continue for new data until its deadline", 2: "keep it stopped"}.get(option, "choose from its match alert")
        return {1: "continue for 1 more hour", 2: "continue for 2 more hours",
                3: "continue for 4 more hours", 4: "keep it stopped"}.get(option, "choose from its expiry alert")
    summary = ("More than one tracker has a pending choice. Which tracker should use your previous choice "
               f"(option {option})? Reply with its number:\n" +
               "\n".join(f"{index}. {row['name']} — {action(row)}; deadline {local_time(row['expires_at'])}."
                         for index, row in enumerate(choices, 1)))
    return summary, ToolResult("expiry-target-choice", "resume_live_tracker", "failed", {
        "reason": "live_tracker_choice_ambiguous", "selected_option": option,
        "tracker_choices": choices,
        **({"anchor_turn_id": anchor_turn_id} if isinstance(anchor_turn_id, str) else {}),
    }, summary)


def _pending_tracker_choices(store, history, principal_id, conversation_id):
    from nullion.connections import workspace_id_for_principal
    from nullion.live_tracking import timestamp, utcnow
    pending = {}
    for message in reversed(history):
        candidate = (message.get("metadata") or {}).get("live_tracker_extension_choice")
        if not isinstance(candidate, str):
            continue
        offer = store.extension_offer(candidate)
        if (not offer or offer["selected"] is not None or offer["workspace_id"] != workspace_id_for_principal(principal_id)
                or offer["conversation_id"] != conversation_id or offer["valid_until"] <= timestamp(utcnow())):
            continue
        job = store.get(offer["tracker_id"])
        if job and job["state"] in {"expired", "matched"} and job.get("extension_offer_id") == candidate:
            pending[candidate] = {"choice_id":candidate, "choice_kind":offer["kind"],
                                  "name":job["name"], "expires_at":job["expires_at"]}
    return list(pending.values())


def expiry_choice_reply(runtime, *, prompt, conversation_id, principal_id, reply_context=None):
    resolution = {}
    try:
        return _expiry_choice_reply(runtime, prompt=prompt, conversation_id=conversation_id,
            principal_id=principal_id, reply_context=reply_context, resolution=resolution)
    except Exception as exc:
        if not is_storage_busy(exc):
            raise
        if isinstance(resolution.get("tracker_choices"), list):
            prompt, result = _choose_tracker_reply(resolution["tracker_choices"], resolution["selected_option"],
                anchor_turn_id=resolution.get("anchor_turn_id"))
            prompt = "Storage is busy, so that choice was not applied. Please choose the tracker again.\n\n" + prompt
            return prompt, ToolResult(result.invocation_id, result.tool_name, result.status, result.output, prompt)
        summary = (
            "I couldn’t confirm that choice while storage is busy. Please reply with the same number again in a moment; "
            "an extension will only be applied once."
        )
        return summary, ToolResult("expiry-storage-busy", "resume_live_tracker", "failed",
            {"reason":"live_tracker_storage_busy", **resolution}, summary)


def _expiry_choice_reply(runtime, *, prompt, conversation_id, principal_id, reply_context=None, resolution):
    selected = str(prompt or "").strip()
    if not selected.isdecimal() or len(selected) > 2 or not conversation_id:
        return None
    from nullion.chat_store import get_chat_store
    from nullion.connections import workspace_id_for_principal
    from nullion.live_tracking_tools import tracker_store_for_registry

    context = reply_context if isinstance(reply_context, dict) else {}
    explicit = bool(context.get("reply_to_message_id") or context.get("reply_to_text"))
    history = get_chat_store().load_messages(conversation_id, limit=100 if explicit else 32)
    if not explicit:
        last = runtime.store.list_recent_conversation_events(conversation_id, event_type="conversation.chat_turn", limit=1)
        latest_bot = next((m for m in reversed(history) if m.get("role") == "bot"), None)
        if last:
            previous = last[-1]
            menu = next((r.get("output") or {} for r in previous.get("tool_results") or []
                         if r.get("tool_name") == "resume_live_tracker" and r.get("status") == "failed"
                         and (r.get("output") or {}).get("reason") == "live_tracking_needs_details"
                         and isinstance((r.get("output") or {}).get("extension_choice_id"), str)), None)
            # The committed turn is authoritative even when the client's
            # secondary history save has not arrived. Never revive a menu
            # after a newer background notice or unrelated bot response.
            if menu and previous.get("assistant_reply") == menu.get("clarification"):
                from datetime import datetime, timezone
                def sent_at(value):
                    try:
                        return datetime.fromisoformat(value)
                    except (TypeError, ValueError):
                        return datetime.min.replace(tzinfo=timezone.utc)
                metadata = (latest_bot or {}).get("metadata") or {}
                if (not latest_bot or metadata.get("runtime_turn_id") == previous.get("turn_id")
                        or sent_at(latest_bot.get("created_at")) <= sent_at(previous.get("created_at"))):
                    latest_bot = {"role":"bot", "text":previous["assistant_reply"],
                        "created_at":previous["created_at"], "metadata":{
                            "runtime_turn_id":previous.get("turn_id"),
                            "live_tracker_extension_choice":menu["extension_choice_id"],
                            "live_tracker_choice_created_at":previous["created_at"],
                        }}
                    history = [*history, latest_bot]
        if last and latest_bot:
            previous = last[-1]
            linked_turn = ((latest_bot.get("metadata") or {}).get("runtime_turn_id") == previous.get("turn_id")
                           or latest_bot.get("text") == previous.get("assistant_reply"))
            pending = next((r.get("output") or {} for r in previous.get("tool_results") or []
                    if r.get("tool_name") == "resume_live_tracker" and r.get("status") == "failed"
                    and (r.get("output") or {}).get("reason") == "live_tracker_choice_ambiguous"
                    and isinstance((r.get("output") or {}).get("tracker_choices"), list)), None)
            legacy_ambiguity = linked_turn and pending is None and any(
                r.get("tool_name") == "resume_live_tracker" and r.get("status") == "failed"
                and (r.get("output") or {}).get("reason") == "live_tracker_choice_ambiguous"
                for r in previous.get("tool_results") or [])
            previous_option = str(previous.get("user_message") or "").strip()
            if legacy_ambiguity and previous_option in {"1", "2", "3", "4"}:
                from nullion.live_tracking_tools import tracker_store_for_registry
                store = tracker_store_for_registry(getattr(runtime, "active_tool_registry", None))
                choices = _pending_tracker_choices(store, history, principal_id, conversation_id)
                if choices:
                    return _choose_tracker_reply(choices, int(previous_option), anchor_turn_id=previous.get("turn_id"))
            if pending:
                menu_id = (latest_bot.get("metadata") or {}).get("live_tracker_extension_choice")
                linked_turn = linked_turn or (pending.get("anchor_turn_id") is not None and
                    (latest_bot.get("metadata") or {}).get("runtime_turn_id") == pending["anchor_turn_id"])
                linked_turn = linked_turn or (isinstance(menu_id, str) and any(
                    row.get("choice_id") == menu_id for row in pending["tracker_choices"]))
                if linked_turn:
                    choices, option = pending["tracker_choices"], pending.get("selected_option")
                    if type(option) is int and option in {1, 2, 3, 4}:
                        if not 1 <= int(selected) <= len(choices):
                            return _choose_tracker_reply(choices, option, anchor_turn_id=pending.get("anchor_turn_id"))
                        resolution.update(tracker_choices=choices, selected_option=option, anchor_turn_id=pending.get("anchor_turn_id"))
                        from nullion.live_tracking_tools import tracker_store_for_registry
                        store = tracker_store_for_registry(getattr(runtime, "active_tool_registry", None))
                        offer = store.extension_offer(choices[int(selected)-1]["choice_id"])
                        if offer:
                            # The user selected a target for a previously chosen
                            # action. Preserve the durable offer as its explicit
                            # anchor, and revalidate scope/state atomically below.
                            return _expiry_choice_reply(runtime, prompt=str(option), conversation_id=conversation_id,
                                principal_id=principal_id, resolution=resolution, reply_context={
                                    "reply_to_from_bot": True, "reply_to_message_id": offer["notice_id"],
                                    "reply_to_text": offer["text"],
                                    "live_tracker_choice_id": offer["id"],
                                })
            retry_id = next((
                (r.get("output") or {}).get("extension_choice_id")
                for r in previous.get("tool_results") or []
                if r.get("tool_name") == "resume_live_tracker" and r.get("status") == "failed"
                and (r.get("output") or {}).get("reason") in {"live_tracker_storage_busy", "live_tracking_needs_details"}
            ), None)
            metadata = latest_bot.get("metadata") or {}
            linked = (metadata.get("runtime_turn_id") == previous.get("turn_id")
                      or latest_bot.get("text") == previous.get("assistant_reply")
                      or metadata.get("live_tracker_extension_choice") == retry_id)
            if isinstance(retry_id, str) and linked:
                # Keep a failed, unconsumed UI choice selectable after its
                # friendly busy response, without interpreting response prose.
                history = [*history, {**latest_bot, "metadata": {
                    **metadata, "live_tracker_extension_choice": retry_id,
                    "live_tracker_choice_created_at": previous["created_at"],
                }}]
    if explicit:
        if context.get("reply_to_from_bot") is not True:
            return None
        anchor = str(context.get("reply_to_text") or "").strip()
        choice_id = context.get("live_tracker_choice_id")
        messages = [m for m in reversed(history) if m.get("role") == "bot"
                    and ((m.get("metadata") or {}).get("live_tracker_extension_choice") == choice_id
                         if isinstance(choice_id, str) else str(m.get("text") or "").strip() == anchor)]
    else:
        from datetime import datetime, timezone
        turns = runtime.store.list_recent_conversation_events(conversation_id, event_type="conversation.chat_turn", limit=32)
        turn_times = {t.get("turn_id"): t.get("created_at") for t in turns if t.get("turn_id")}

        def authored_at(message):
            metadata = message.get("metadata") or {}
            value = (metadata.get("live_tracker_choice_created_at")
                     or turn_times.get(metadata.get("runtime_turn_id"))
                     or message.get("created_at"))
            try:
                return datetime.fromisoformat(value)
            except (TypeError, ValueError):
                return datetime.min.replace(tzinfo=timezone.utc)

        # Client persistence can arrive after a newer background notice. Use
        # the verified originating turn time rather than the insertion order.
        notices = [m for m in history if (m.get("metadata") or {}).get("live_tracker_extension_choice")]
        latest_notice_time = max((authored_at(m) for m in notices), default=None)
        messages = []
        for candidate in reversed(history):
            if candidate.get("role") != "bot":
                continue
            origin = (candidate.get("metadata") or {}).get("runtime_turn_id")
            if origin in turn_times and latest_notice_time and authored_at(candidate) < latest_notice_time:
                continue  # A verified older turn was persisted late.
            messages = [candidate]
            break
        if messages and turns:
            try:
                if datetime.fromisoformat(turns[-1]["created_at"]) > authored_at(messages[0]):
                    return None
            except (KeyError, TypeError, ValueError):
                return None
    if not messages:
        return None
    offer_id = (messages[0].get("metadata") or {}).get("live_tracker_extension_choice")
    if not isinstance(offer_id, str):
        return None
    resolution["extension_choice_id"] = offer_id
    registry = getattr(runtime, "active_tool_registry", None)
    store = tracker_store_for_registry(registry)
    if not explicit:
        # A current tool-generated renewal menu already selects its exact
        # tracker. Other expired background menus cannot override that target.
        bound_menu = next((r.get("output") or {} for r in (last[-1].get("tool_results") or [])
                          if r.get("tool_name") == "resume_live_tracker" and r.get("status") == "failed"
                          and (r.get("output") or {}).get("reason") == "live_tracking_needs_details"
                          and (r.get("output") or {}).get("extension_choice_id") == offer_id), None) if last else None
        selected_target = bool(bound_menu and last[-1].get("assistant_reply") == bound_menu.get("clarification"))
        pending = _pending_tracker_choices(store, history, principal_id, conversation_id)
        if len(pending) > 1 and not selected_target:
            return _choose_tracker_reply(pending, int(selected))
    try:
        job, changed = store.choose_extension(
            offer_id, int(selected), workspace_id_for_principal(principal_id), conversation_id,
        )
        if not changed:
            summary = f"That choice was already handled. No additional time was added. Live tracker “{job['name']}” is {job['state']}."
        elif job["state"] == "stopped":
            summary = f"Live tracker “{job['name']}” will stay stopped."
        else:
            summary = (
                f"Live tracker “{job['name']}” is active again until {local_time(job['expires_at'])}. "
                f"Checking every {job['interval_seconds']} seconds. "
                + ('Stopping after the first match. I’ll offer to continue after that match. '
                   if job.get('stop_after_match', True) else
                   'I’ll keep checking and alert on new changes until the end time. ')
                + "I’ll notify you when this window ends."
            )
        wake = getattr(registry, "live_tracker_wake", None)
        if callable(wake) and changed:
            wake()
        output = {**public_tracker(job), "message": summary, "action_receipt": _action_receipt(
            action="extended" if job["state"] == "active" and changed else job["state"],
            object_type="live_tracker", object_id=job["id"], object_name=job["name"],
            summary=summary, delivery_required=True,
        )}
        return summary, ToolResult("expiry-choice-" + offer_id, "resume_live_tracker", "completed", output)
    except ContinuationWindowEnded:
        offer = store.extension_offer(offer_id)
        renewal = store.renewal_offer(offer["tracker_id"], workspace_id_for_principal(principal_id), conversation_id)
        summary = renewal["text"]
        return summary, ToolResult("match-window-ended-" + offer_id, "resume_live_tracker", "failed", {
            "reason": "live_tracking_needs_details", "clarification": summary,
            "tracker_id": offer["tracker_id"], "extension_choice_id": renewal["id"],
        }, "The original tracking window ended. Choose a new duration.")
    except ValueError as exc:
        summary = str(exc)
        return summary, ToolResult("expiry-choice-" + offer_id, "resume_live_tracker", "failed", {"reason":"live_tracker_choice_unavailable"}, summary)

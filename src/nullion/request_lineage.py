"""Resolve selected request context by stored turn identity, never quoted prose."""
from collections.abc import Mapping
from datetime import datetime
from copy import copy


def recent_context_only_turns(store, turn, workspace_id, *, exclude_ids=()):
    """Recover source facts behind an older broken root, never its task/consent."""
    recent = getattr(store, 'list_recent_conversation_events_after_reset', None)
    if not callable(recent): return []
    rows = recent(turn.conversation_id, event_type='conversation.chat_turn', limit=6)
    selected, remaining = [], 12000
    for row in reversed(list(rows or ())):
        if (not isinstance(row, Mapping) or row.get('conversation_id') != turn.conversation_id
                or row.get('workspace_id') != workspace_id or not row.get('turn_id')
                or row['turn_id'] in exclude_ids):
            continue
        user = str(row.get('user_message') or '')[:min(1000,remaining)]
        assistant = str(row.get('assistant_reply') or '')[:min(4000,max(0,remaining-len(user)))]
        selected.append({'turn_id':row['turn_id'],'created_at':row.get('created_at'),
            'user':user,'assistant':assistant,'context_only':True})
        remaining -= len(user)+len(assistant)
        if remaining <= 0: break
    return list(reversed(selected))


def selected_ancestor_turns(store, turn):
    """Traverse only the selected typed request lineage, including revised inputs."""
    if turn is None:
        return []
    disposition = str(getattr(turn.disposition, "value", turn.disposition))
    if not turn.parent_turn_id or disposition not in {"continue", "revise", "background_follow_up"}:
        return []
    reset_at = None
    recent = getattr(store, "list_recent_conversation_events", None)
    if callable(recent):
        resets = recent(turn.conversation_id, event_type="conversation.session_reset", limit=1)
        if resets:
            try:
                reset_at = datetime.fromisoformat(str(resets[-1].get("created_at") or "").replace("Z", "+00:00")).timestamp()
            except (ValueError, TypeError):
                return []
    parents, seen = [], {turn.turn_id}
    selected_branches = {turn.branch_id}
    parent_id = turn.parent_turn_id
    for _ in range(12):
        if not parent_id or parent_id in seen:
            break
        seen.add(parent_id)
        parent = store.get_conversation_turn(parent_id)
        if parent is None or parent.conversation_id != turn.conversation_id:
            break
        if reset_at is not None:
            try:
                if parent.created_at.timestamp() <= reset_at:
                    break
            except (ValueError, TypeError, AttributeError):
                break
        branch = store.get_conversation_branch(parent.branch_id)
        branch_status = str(getattr(getattr(branch, "status", None), "value", getattr(branch, "status", "")))
        if branch_status == "cancelled":
            break
        if str(getattr(parent.status, "value", parent.status)) == "cancelled" and not (
                branch_status == "superseded" and getattr(branch, "superseded_by_branch_id", None) in selected_branches):
            break
        parents.append(parent)
        selected_branches.add(parent.branch_id)
        parent_id = parent.parent_turn_id
    return parents


def selected_request_context(store, turn, workspace_id):
    """A bounded, reset-aware ancestry read only for an explicit continuation.

    History explains the current request; it cannot authorize an action or
    override a cancellation, revision or replacement in the current message.
    """
    parent = getattr(turn, 'parent_turn_id', None)
    disposition = getattr(turn, 'disposition', None)
    if not parent or disposition not in {'continue', 'revise', 'background_follow_up', 'interrupt'}:
        return None
    recent = getattr(store, 'list_recent_conversation_events_after_reset', None)
    if not callable(recent):
        return None
    ancestry = turn
    if disposition == 'interrupt':
        # Replacement cancels the old goal, not the observed source details
        # explicitly linked to it. This reference-only view cannot grant action
        # continuation or revive the old task's requirements.
        ancestry = copy(turn)
        ancestry.disposition = 'continue'
    allowed = {ancestor.turn_id:ancestor for ancestor in selected_ancestor_turns(store, ancestry)}
    events = recent(turn.conversation_id, event_type='conversation.chat_turn', limit=50)
    indexed = {row.get('turn_id'):row for row in events or () if isinstance(row, Mapping)
        and row.get('conversation_id') == turn.conversation_id}
    selected, seen, identity, truncated = [], set(), parent, False
    reference_only = disposition == 'interrupt'
    original = getattr(turn, 'user_message', None) if reference_only else None
    text_truncated, remaining = False, 12000
    for _ in range(8):
        row = indexed.get(identity)
        ancestor = allowed.get(identity)
        if (identity in seen or ancestor is None
                or row is not None and row.get('workspace_id') != workspace_id):
            truncated = True
            break
        seen.add(identity)
        user = ancestor.user_message
        assistant = row.get('assistant_reply','') if row is not None else ''
        if not isinstance(user,str) or not user.strip() or not isinstance(assistant,str):
            truncated = True
            break
        if not reference_only:
            original = user if len(user) <= 12000 else None
        user_cap = min(1000, remaining)
        assistant_cap = min(4000, max(0, remaining-min(len(user),user_cap)))
        text_truncated |= len(user) > user_cap or len(assistant) > assistant_cap
        selected.append({'turn_id':identity, 'user':user[:user_cap], 'assistant':assistant[:assistant_cap],
            **({'context_only':True} if reference_only else {})})
        remaining -= len(selected[-1]['user']) + len(selected[-1]['assistant'])
        relationship = getattr(ancestor,'disposition',None)
        if relationship not in {'continue','revise','background_follow_up','interrupt'}:
            break
        if relationship == 'interrupt':
            reference_only = True
        identity = ancestor.parent_turn_id
        if not identity:
            break
        if remaining <= 0:
            text_truncated = True
            break
    else:
        truncated = True
    if not selected:
        return None
    selected.reverse()
    return {'parent_turn_id':parent, 'original_request':original if not truncated else None,
        'turns':selected, 'truncated':truncated or text_truncated,
        **({'goal_replaced':True} if disposition == 'interrupt' else {})}

"""Shared browser runtime configuration."""
from __future__ import annotations

import os
from pathlib import Path


DEFAULT_AGENT_BROWSER_SESSION_ID = "agent-browser-default"

_RECOVERY_TOOLS = frozenset({'browser_open','browser_navigate','browser_extract_text',
    'browser_extract_items','browser_extract_detail','browser_snapshot','browser_find',
    'browser_screenshot','browser_read_pages'})


def browser_recovery_receipts(results):
    """Retain a native choice without copying private source/tool output."""
    receipts = []
    for result in results or ():
        name = result.get('tool_name') if isinstance(result, dict) else getattr(result,'tool_name',None)
        status = result.get('status') if isinstance(result, dict) else getattr(result,'status',None)
        output = result.get('output') if isinstance(result, dict) else getattr(result,'output',None)
        invocation = result.get('invocation_id') if isinstance(result, dict) else getattr(result,'invocation_id',None)
        if name not in _RECOVERY_TOOLS or status != 'failed' or not invocation or not isinstance(output,dict):
            continue
        if (output.get('result_kind')!='user_choice_required' or output.get('failure_reason')!='browser_connection_unavailable'
                or output.get('recovery_tool')!='browser_open' or output.get('requires_user_choice') is not True):
            continue
        options=[{'number':row['number'],'mode':row['mode']} for row in output.get('recovery_options') or ()
            if isinstance(row,dict) and type(row.get('number')) is int and row['number']>0
            and row.get('mode') in {'configured','headless'}]
        if options:
            receipts.append({'invocation_id':invocation,'tool_name':name,'status':'failed','output':{
                'result_kind':'user_choice_required','failure_reason':'browser_connection_unavailable',
                'recovery_tool':'browser_open','requires_user_choice':True,'recovery_options':options}})
    return receipts


def selected_browser_recovery(store, *, reply, conversation_id, parent_turn_id, principal_id):
    """Bind a numeric reply to a native recovery choice, never assistant prose."""
    if not parent_turn_id or len(reply.strip()) > 8:
        return None
    try:
        number = int(reply.strip())
    except ValueError:
        return None
    if number < 1:
        return None
    getter = getattr(store, 'list_recent_conversation_events_after_reset', None)
    if not callable(getter):
        return None
    events = getter(conversation_id, event_type='conversation.chat_turn', limit=25)
    for event in reversed(events):
        if event.get('turn_id') != parent_turn_id:
            continue
        for result in reversed(browser_recovery_receipts(event.get('tool_results'))):
            output = result.get('output') or {}
            if (not isinstance(output, dict) or output.get('result_kind') != 'user_choice_required'
                    or output.get('failure_reason') != 'browser_connection_unavailable'
                    or output.get('recovery_tool') != 'browser_open'
                    or output.get('requires_user_choice') is not True):
                continue
            for option in output.get('recovery_options') or []:
                if isinstance(option, dict) and option.get('number') == number and option.get('mode') in {'configured','headless'}:
                    return {'mode':option['mode'], 'source_turn_id':parent_turn_id,
                        'source_invocation_id':result['invocation_id'], 'principal_id':principal_id}
    return None


def shared_browser_cdp_url() -> str | None:
    """All controllers prefer the configured managed-browser endpoint.

    The older SDK-only setting remains a compatibility fallback when the
    managed browser has no explicit endpoint; it must not override that session.
    """
    legacy = str(os.environ.get("NULLION_BROWSER_USE_CDP_URL") or "").strip()
    return (str(os.environ.get("NULLION_BROWSER_CDP_URL") or "").strip()
        or legacy or "http://127.0.0.1:9222")


def nullion_runtime_home() -> Path:
    """Return the active Nullion lane home used for browser state."""
    configured_home = str(os.environ.get("NULLION_HOME") or "").strip()
    if configured_home:
        return Path(configured_home).expanduser()
    data_dir = str(os.environ.get("NULLION_DATA_DIR") or "").strip()
    if data_dir:
        return Path(data_dir).expanduser()
    checkpoint = str(
        os.environ.get("NULLION_CHECKPOINT_PATH")
        or os.environ.get("NULLION_CHECKPOINT")
        or ""
    ).strip()
    if checkpoint:
        return Path(checkpoint).expanduser().parent
    return Path.home() / ".nullion"


def browser_user_data_dir(browser_kind: str) -> Path:
    configured = str(os.environ.get("NULLION_BROWSER_USER_DATA_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    safe_kind = "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in browser_kind).strip("-")
    return nullion_runtime_home() / "browser-profiles" / (safe_kind or "chrome")

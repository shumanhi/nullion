"""Exact native terminal-event projection; never resume or execute work."""
from contextlib import closing
import json
import logging
from pathlib import Path
import sqlite3

logger = logging.getLogger(__name__)


def project_terminal_chat_turn(event, *, store=None):
    if (not isinstance(event, dict) or event.get('event_type') != 'conversation.chat_turn'
            or event.get('history_state') != 'terminal' or not event.get('turn_id')
            or not event.get('conversation_id')):
        raise ValueError('A native terminal turn identity is required')
    if store is None:
        from nullion.chat_store import get_chat_store
        store = get_chat_store()
    if not store.has_terminal_projection(event):
        store.import_runtime_chat_turns([event], timeout_seconds=0.1, _exact_terminal_identity=True)


def repair_selected_terminal_history(runtime, conversation_id, store):
    """Repair a bounded batch for one selected conversation.

    The indexed lookup is independent of another process's in-memory runtime.
    Explicit destructive history resets remain authoritative. This path is only
    used by history reads, never by ordinary request admission or tool execution.
    """
    path = getattr(runtime, 'checkpoint_path', None)
    if path is None or Path(path).suffix.lower() not in {'.db', '.sqlite', '.sqlite3'}:
        return
    try:
        source = str(Path(path).resolve())
        cursor = store.terminal_projection_cursor(source, conversation_id)
        with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=0.1)) as db:
            selected = """ FROM conversation_events
                WHERE collection='conversation_events'
                  AND json_extract(payload,'$.conversation_id')=?
                  AND json_extract(payload,'$.event_type')='conversation.chat_turn'
                  AND json_extract(payload,'$.history_state')='terminal'"""
            latest = db.execute('SELECT item_key,payload' + selected +
                " ORDER BY json_extract(payload,'$.created_at') DESC,item_key DESC LIMIT 1", (conversation_id,)).fetchone()
            if latest is None:
                return
            batch_query = 'SELECT item_key,payload' + selected + """
                AND (json_extract(payload,'$.created_at'),item_key)>(?,?)
                ORDER BY json_extract(payload,'$.created_at'),item_key LIMIT 8"""
            batch = db.execute(batch_query, (conversation_id, *cursor)).fetchall()
            if not batch:
                # Wrapping also covers a late persistence write with an older
                # event timestamp. A fixed newest tail would starve that row.
                batch = db.execute(batch_query, (conversation_id, '', '')).fetchall()
            reset = db.execute("""SELECT payload FROM conversation_events
                WHERE collection='conversation_events'
                  AND json_extract(payload,'$.conversation_id')=?
                  AND json_extract(payload,'$.event_type')='conversation.session_reset'
                  AND NOT (json_extract(payload,'$.history_retained') IS 1
                    OR (json_type(payload,'$.history_retained') IS NULL
                        AND json_extract(payload,'$.chat_id') IS NOT NULL))
                ORDER BY json_extract(payload,'$.created_at') DESC LIMIT 1""", (conversation_id,)).fetchone()
        from nullion.session_stop import retained_history_turns
        for _, raw in dict([latest, *batch]).items():
            event = json.loads(raw)
            if reset is not None and not retained_history_turns([event, json.loads(reset[0])]):
                continue
            project_terminal_chat_turn(event, store=store)
        if batch:
            last_key, last_raw = batch[-1]
            store.save_terminal_projection_cursor(source, conversation_id,
                (json.loads(last_raw)['created_at'], last_key))
    except Exception as exc:
        # Canonical outcomes remain durable; the next selected history read
        # retries projection without repeating a provider or native action.
        logger.debug('Unable to repair selected terminal history (%s)', type(exc).__name__)

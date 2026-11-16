"""Workspace task records with durable, reversible updates and tool receipts."""
from __future__ import annotations

from nullion.sqlite_connections import sqlite_connection

from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from nullion.tools import ToolInvocation, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

_EVENT_TYPE = "workspace.personal_task"
_NAMES = frozenset({"personal_tasks_list", "personal_task_save"})
_FIELDS = {
    "operation": {"type": "string", "enum": ["create", "update"],
                  "description": "Distinguish adding a new personal task from changing an existing task. A status report is not automatically a request to create a task."},
    "title": {"type": "string", "minLength": 1, "maxLength": 1000},
    "status": {"type": "string", "enum": ["pending", "completed", "archived"]},
    "priority": {"type": "string", "enum": ["low", "normal", "high"]},
    "due_date": {"type": ["string", "null"], "description": "Optional ISO calendar date, YYYY-MM-DD."},
    "task_id": {"type": "string", "description": "Existing task id for an update; omit for an addition."},
    "target_title": {"type":"string", "minLength":1, "maxLength":1000,
                     "description":"Required for an update: the existing task title resolved from the user's intended target, before any rename. If the requested target does not match a listed record, do not substitute another item merely because it is the only pending task; ask a numbered clarification."},
    "target_reference": {"type":"string","minLength":1,"maxLength":1000,
        "description":"Quote the user's task name or unique name fragment from the current request or its verified selected user turns. Do not substitute a different listed item when the named task is absent."},
    "expected_revision": {"type": "integer", "minimum": 1, "description": "Required for an update; take from personal_tasks_list."},
    "source_turn_id": {"type": "string", "description": "For restoring a previously requested item, its saved user turn id."},
}
_SPECS = {
    "personal_tasks_list": ToolSpec(
        "personal_tasks_list", "Read the current workspace's persistent personal to-do items. Completed and archived items remain stored. "
        "An empty durable list does not prove earlier chat items never existed: use chat_history_search before claiming that.",
        ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5,
        input_schema={"type": "object", "properties": {"status": {"type": "string", "enum": ["pending", "completed", "archived", "all"]}}, "additionalProperties": False},
        capability_tags=("personal_tasks", "account_read")),
    "personal_task_save": ToolSpec(
        "personal_task_save", "Persist one user-authorized personal task addition or update. Requires title for a new task; "
        "updates require task_id, expected_revision and target_title identifying the same intended task from the list. Archive removes an item from the pending list reversibly. "
        "Completing an existing item must use its task_id; an empty list is not permission to create a completed item. "
        "Only use operation=create with a terminal status when the user explicitly asks to log a new completed/archived task. "
        "For a status report with no identified task, ask whether they want a task changed instead of inventing one. "
        "Only claim saved/updated after persisted=true. This does not create a scheduled reminder or external task.",
        ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5,
        input_schema={"type": "object", "properties": _FIELDS,
                      "anyOf": [{"required": ["title"]}, {"required": ["task_id", "expected_revision", "target_title"]}], "additionalProperties": False},
        capability_tags=("personal_tasks",)),
}


def _target_reference_is_grounded(invocation,previous,records):
    """Bind a selected native identity to owned user data, without classifying intent."""
    context=invocation.flow_context or {}
    if 'current_user_request' not in context:
        return True  # Non-chat native callers already supply an explicit record.
    sources=[context.get('current_user_request')]
    selected=context.get('selected_request_context')
    if isinstance(selected,dict) and selected.get('parent_turn_id'):
        sources.extend(row.get('user') for row in selected.get('turns',[])
            if isinstance(row,dict) and not row.get('context_only'))
    normalize=lambda value:' '.join(str(value or '').split()).casefold()
    reference=normalize(invocation.arguments.get('target_reference') or invocation.arguments.get('target_title'))
    if not reference or not any(reference in normalize(value) for value in sources if isinstance(value,str)):
        return False
    if reference==normalize(previous['task_id']):return True
    matched=[row['task_id'] for row in records.values() if reference in normalize(row['title'])]
    return matched==[previous['task_id']]


def _records(connection, conversation_id):
    rows = connection.execute(
        "SELECT payload FROM conversation_events WHERE collection='conversation_events' "
        "AND json_extract(payload,'$.conversation_id')=? AND json_extract(payload,'$.event_type')=? ORDER BY rowid",
        (conversation_id, _EVENT_TYPE))
    records = {}
    for payload, in rows:
        event = json.loads(payload)
        record = event['task']
        previous = records.get(record['task_id'])
        if previous is None or record['revision'] > previous['revision']:
            records[record['task_id']] = record
    return records


class PersonalTaskToolRegistry:
    def __init__(self, delegate, *, runtime, workspace_id):
        self._delegate = delegate
        self._runtime = runtime
        self._workspace_id = workspace_id
        self._conversation_id = f"workspace:{workspace_id}:personal_tasks"

    def get_spec(self, name):
        return _SPECS[name] if name in _NAMES else self._delegate.get_spec(name)

    def list_specs(self):
        return [*self._delegate.list_specs(), *_SPECS.values()]

    def list_tool_definitions(self, *args, **kwargs):
        definitions = list(self._delegate.list_tool_definitions(*args, **kwargs))
        definitions.extend({"name": spec.name, "description": spec.description, "input_schema": spec.input_schema,
                            "capability_tags": list(spec.capability_tags), "side_effect_class": spec.side_effect_class.value,
                            "risk_level": spec.risk_level.value, "requires_approval": False} for spec in _SPECS.values())
        return definitions

    def can_invoke_tool(self, name):
        if name in _NAMES:
            return True
        predicate = getattr(self._delegate, 'can_invoke_tool', None)
        return bool(predicate(name)) if callable(predicate) else any(spec.name == name for spec in self._delegate.list_specs())

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    def invoke(self, invocation: ToolInvocation):
        if invocation.tool_name not in _NAMES:
            return self._delegate.invoke(invocation)
        path = Path(getattr(self._runtime, 'checkpoint_path', ''))
        if path.suffix.lower() not in {'.db', '.sqlite', '.sqlite3'}:
            return self._failure(invocation, 'persistent_storage_unavailable', 'Persistent task storage is unavailable.')
        if invocation.tool_name == 'personal_task_save':
            try:
                initialized = False
                if path.is_file():
                    with sqlite_connection(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5) as connection:
                        initialized = connection.execute(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='conversation_events'"
                        ).fetchone() is not None
                if not initialized:
                    from nullion.runtime_persistence import _sqlite_runtime_ddl, RUNTIME_STORE_FORMAT_VERSION
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with sqlite_connection(str(path), timeout=5) as connection:
                        connection.executescript(_sqlite_runtime_ddl())
                        connection.execute('INSERT OR IGNORE INTO runtime_meta (key,value) VALUES (?,?)',
                            ('format_version', str(RUNTIME_STORE_FORMAT_VERSION)))
            except (ValueError, TypeError, sqlite3.Error, OSError) as exc:
                return self._failure(invocation, 'persistent_storage_unavailable', str(exc))
        if not path.is_file() and invocation.tool_name != 'personal_tasks_list':
            return self._failure(invocation, 'persistent_storage_unavailable', 'Persistent task storage is unavailable.')
        args = invocation.arguments or {}
        try:
            if invocation.tool_name == 'personal_tasks_list':
                status = args.get('status', 'pending')
                if status not in {'pending', 'completed', 'archived', 'all'}:
                    raise ValueError('Unsupported task status.')
                if not path.is_file():
                    return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
                        {'workspace_id': self._workspace_id, 'items': [], 'item_count': 0,
                         'persisted': True, 'storage_initialized': False})
                with sqlite_connection(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5) as connection:
                    initialized = connection.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='conversation_events'"
                    ).fetchone() is not None
                    records = _records(connection, self._conversation_id) if initialized else {}
                items = [record for record in records.values() if status == 'all' or record['status'] == status]
                return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
                                  {'workspace_id': self._workspace_id, 'items': items, 'item_count': len(items), 'persisted': True})
            return self._save(invocation, path, args)
        except (ValueError, TypeError, sqlite3.Error, OSError) as exc:
            return self._failure(invocation, 'task_storage_or_argument_error', str(exc))

    def _failure(self, invocation, reason, error):
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {'reason': reason, 'persisted': False}, error=error)

    def _save(self, invocation, path, args):
        now = datetime.now(UTC).isoformat()
        source_turn_id = args.get('source_turn_id')
        operation = args.get('operation')
        if operation not in {None, 'create', 'update'}:
            raise ValueError('Unsupported task operation.')
        if operation == 'create' and args.get('task_id'):
            raise ValueError('A task creation cannot also identify an existing task to update.')
        if not args.get('task_id') and (operation == 'update' or
                (args.get('status') in {'completed', 'archived'} and operation != 'create')):
            return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {
                'reason':'personal_task_target_required', 'persisted':False, 'executed':False,
                'result_kind':'user_choice_required', 'requires_user_choice':True,
                'user_question':'No personal task was changed. Should I treat this as a status update or change your personal task list?',
                'recovery_options':[
                    {'number':1, 'label':'Keep it as a status update; leave my personal task list unchanged.'},
                    {'number':2, 'label':'Show my existing personal tasks so I can choose one.'},
                    {'number':3, 'label':'Create a new completed personal task.'}]})
        with sqlite_connection(str(path), timeout=5) as connection:
            connection.execute('BEGIN IMMEDIATE')
            records = _records(connection, self._conversation_id)
            task_id = args.get('task_id')
            if task_id:
                previous = records.get(task_id)
                if previous is None:
                    return self._failure(invocation, 'task_not_found', 'Task not found in this workspace.')
                if type(args.get('expected_revision')) is not int or args['expected_revision'] != previous['revision']:
                    return self._failure(invocation, 'task_revision_conflict', 'Task changed; read the list again before updating.')
                target_title = args.get('target_title')
                if (not isinstance(target_title, str) or
                        ' '.join(target_title.split()).casefold() != ' '.join(previous['title'].split()).casefold()
                        or not _target_reference_is_grounded(invocation,previous,records)):
                    return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {
                        'reason':'personal_task_target_mismatch', 'persisted':False, 'executed':False,
                        'result_kind':'user_choice_required', 'requires_user_choice':True,
                        'user_question':'No personal task was changed. The selected record does not confirm the intended task. What should I do?',
                        'recovery_options':[{'number':1, 'label':'Show the current personal task list.'},
                                            {'number':2, 'label':'Leave the task list unchanged.'}]})
                task = dict(previous)
                mutation = 'updated'
            else:
                title = args.get('title')
                if not isinstance(title, str) or not title.strip():
                    raise ValueError('A new task requires a title.')
                task_id = f'task-{uuid4().hex}'
                if source_turn_id:
                    rows = connection.execute("SELECT payload FROM conversation_events WHERE collection='conversation_events' "
                        "AND json_extract(payload,'$.event_type')='conversation.chat_turn' "
                        "AND json_extract(payload,'$.turn_id')=?", (source_turn_id,)).fetchall()
                    from nullion.conversation_history_tools import _event_matches_workspace
                    if not any(_event_matches_workspace(json.loads(row[0]), workspace_id=self._workspace_id)
                               and json.loads(row[0]).get('user_message') for row in rows):
                        return self._failure(invocation, 'source_turn_not_found', 'Source user turn not found in this workspace.')
                    task_id = 'task-' + hashlib.sha256(f'{self._workspace_id}:{source_turn_id}:{title.strip()}'.encode()).hexdigest()[:32]
                    if task_id in records:
                        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
                                          self._receipt(records[task_id], 'unchanged', restored_already=True))
                task = {'task_id': task_id, 'title': title.strip(), 'status': 'pending', 'priority': 'normal',
                        'due_date': None, 'revision': 0, 'created_at': now}
                if source_turn_id:
                    task['source_turn_id'] = source_turn_id
                mutation = 'created'
            for field in ('title', 'status', 'priority', 'due_date'):
                if field in args:
                    task[field] = args[field]
            if not isinstance(task['title'], str) or not task['title'].strip() or len(task['title']) > 1000:
                raise ValueError('Task title must be nonempty and at most 1000 characters.')
            task['title'] = task['title'].strip()
            if task['status'] not in {'pending', 'completed', 'archived'} or task['priority'] not in {'low', 'normal', 'high'}:
                raise ValueError('Unsupported task status or priority.')
            if task['due_date'] is not None:
                task['due_date'] = date.fromisoformat(task['due_date']).isoformat()
            task.update(revision=task['revision'] + 1, updated_at=now)
            event_id = f'personal-task-event-{uuid4().hex}'
            event = {'event_id': event_id, 'conversation_id': self._conversation_id, 'workspace_id': self._workspace_id,
                     'event_type': _EVENT_TYPE, 'created_at': now, 'task': task}
            connection.execute('INSERT INTO conversation_events (collection,item_key,payload,updated_at) VALUES (?,?,?,?)',
                               ('conversation_events', event_id, json.dumps(event, sort_keys=True), now))
        self._runtime.store.add_conversation_event(event)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', self._receipt(task, mutation))

    def _receipt(self, task, operation, **extra):
        from nullion.tools import _action_receipt
        summary = f'{operation.capitalize()} personal task “{task["title"]}” (status: {task["status"]}).'
        return {'task':task, 'persisted':True, 'operation':operation, **extra,
            'action_receipt':_action_receipt(action=operation, object_type='personal task',
                object_id=task['task_id'], object_name=task['title'], summary=summary)}

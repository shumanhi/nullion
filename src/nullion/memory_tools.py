"""Scoped workspace memory operations with durable, verified receipts."""
from __future__ import annotations

import os

from nullion.memory import UserMemoryKind, _SECRET_OR_TOKEN_RE, memory_entries_for_owner, memory_owner_for_workspace, remember_text_fact
from nullion.tools import ToolInvocation, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

_SPECS = {
    "memory_search": ToolSpec(
        "memory_search", "Read this workspace's durable preferences and facts using a focused topic query. "
        "Use chat_history_search for earlier conversations and document references; memory is not a file inventory.",
        ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 3,
        input_schema={"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 20}}, "additionalProperties": False},
        capability_tags=("conversation_history",)),
    "memory_save": ToolSpec(
        "memory_save", "Persist an explicitly requested durable workspace preference or fact. "
        "Quote the current human request as source_quote. Never claim it is saved until persisted is true. "
        "Do not save tool inventories, temporary failures, file contents or credentials as personal memory.",
        ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5,
        input_schema={"type": "object", "properties": {
            "key": {"type": "string", "minLength": 1, "maxLength": 60}, "value": {"type": "string", "minLength": 1, "maxLength": 2000},
            "kind": {"type": "string", "enum": [kind.value for kind in UserMemoryKind]},
            "source_quote": {"type": "string", "minLength": 1}},
            "required": ["key", "value", "kind", "source_quote"], "additionalProperties": False},
        capability_tags=("conversation_history",)),
}


class MemoryToolRegistry:
    def __init__(self, delegate, *, runtime, workspace_id, conversation_id, user_message=None):
        self._delegate = delegate
        self._runtime = runtime
        self._owner = memory_owner_for_workspace(workspace_id)
        self._conversation_id = conversation_id
        if user_message is None:
            head_reader = getattr(runtime.store, 'get_conversation_head', None)
            turn_reader = getattr(runtime.store, 'get_conversation_turn', None)
            head = (head_reader(conversation_id) or {}) if callable(head_reader) else {}
            turn = turn_reader(head.get('active_turn_id', '')) if callable(turn_reader) else None
            user_message = getattr(turn, 'user_message', '')
        self._source_user_message = str(user_message or '')

    def get_spec(self, name):
        return _SPECS[name] if name in _SPECS else self._delegate.get_spec(name)

    def list_specs(self):
        return [*self._delegate.list_specs(), *_SPECS.values()]

    def list_tool_definitions(self, *args, **kwargs):
        definitions = list(self._delegate.list_tool_definitions(*args, **kwargs))
        definitions.extend({"name": spec.name, "description": spec.description, "input_schema": spec.input_schema,
                            "capability_tags": list(spec.capability_tags), "side_effect_class": spec.side_effect_class.value,
                            "risk_level": spec.risk_level.value, "requires_approval": False} for spec in _SPECS.values())
        return definitions

    def can_invoke_tool(self, name):
        if name in _SPECS:
            return True
        predicate = getattr(self._delegate, 'can_invoke_tool', None)
        return bool(predicate(name)) if callable(predicate) else any(spec.name == name for spec in self._delegate.list_specs())

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    def invoke(self, invocation: ToolInvocation):
        if invocation.tool_name not in _SPECS:
            return self._delegate.invoke(invocation)

        def failure(reason):
            return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
                              {'persisted': False, 'reason': reason, 'suppress_activity': True}, error=reason)

        if os.environ.get('NULLION_MEMORY_ENABLED', '1').strip().lower() in {'0', 'false', 'off', 'no'}:
            return failure('memory_disabled')
        try:
            entries = memory_entries_for_owner(self._runtime.store, self._owner, checkpoint_path=getattr(self._runtime, 'checkpoint_path', None))
        except Exception:
            return failure('memory_recall_unavailable')
        args = invocation.arguments
        if invocation.tool_name == 'memory_search':
            from nullion.builder_memory import select_memory_entries_for_prompt, _memory_query_scores
            try:
                limit = max(1, min(20, int(args.get('limit', 10))))
            except (TypeError, ValueError):
                return failure('invalid_limit')
            query = str(args.get('query') or '')
            selected = select_memory_entries_for_prompt(entries, query=query)
            if query:
                scores = _memory_query_scores(entries, query)
                selected = [entry for entry in selected if scores.get(entry.entry_id, 0) > 0]
            return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
                {'matches': [{'memory_id': entry.entry_id, 'key': entry.key, 'value': entry.value, 'kind': entry.kind.value}
                             for entry in selected[:limit] if not _SECRET_OR_TOKEN_RE.search(entry.value)], 'suppress_activity': True})
        key = args.get('key')
        value = args.get('value')
        quote = args.get('source_quote')
        if not all(isinstance(item, str) and item.strip() for item in (key, value, quote)) or len(key) > 60 or len(value) > 2000:
            return failure('invalid_memory_fields')
        try:
            kind = UserMemoryKind(args.get('kind'))
        except (TypeError, ValueError):
            return failure('invalid_memory_kind')
        if _SECRET_OR_TOKEN_RE.search(key + ' ' + value):
            return failure('secret_memory_prohibited')
        user_text = self._source_user_message
        if not user_text or ' '.join(quote.split()) not in ' '.join(user_text.split()):
            return failure('unverified_memory_source')
        for existing in entries:
            if existing.entry_id not in getattr(self._runtime.store, '_dirty_memory_entry_ids', ()):
                self._runtime.store._user_memory_collection_for_kind(existing.kind)[existing.entry_id] = existing
        pending_before = set(self._runtime.store._dirty_memory_entry_ids)
        entry = remember_text_fact(self._runtime.store, owner=self._owner, key=key, value=value,
                                  source='explicit_memory_tool', kind=kind)
        previous = next((existing for existing in entries if existing.entry_id == entry.entry_id), None)
        try:
            from nullion.runtime_persistence import save_user_memory_entry
            path = save_user_memory_entry(entry, self._runtime.checkpoint_path)
            if self._runtime.store.get_user_memory_entry(entry.entry_id) is entry:
                self._runtime.store._dirty_memory_entry_ids.discard(entry.entry_id)
                self._runtime.store._memory_reinforcement_only_ids.discard(entry.entry_id)
            self._runtime.store._memory_checkpoint_path = path
        except Exception:
            if self._runtime.store.get_user_memory_entry(entry.entry_id) is entry:
                self._runtime.store.remove_user_memory_entry(entry.entry_id)
                if previous is not None:
                    self._runtime.store.add_user_memory_entry(previous)
                if entry.entry_id not in pending_before:
                    self._runtime.store._dirty_memory_entry_ids.discard(entry.entry_id)
            return failure('memory_persistence_failed')
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'persisted': True, 'memory_id': entry.entry_id, 'key': entry.key, 'kind': entry.kind.value,
             'message': 'Saved the requested memory.', 'suppress_activity': True})

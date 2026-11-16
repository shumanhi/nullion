from copy import deepcopy
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize('setting', ['chat_streaming_enabled', 'thinking_display_enabled'])
@pytest.mark.parametrize('value', [None, True, False])
def test_indexed_session_settings_never_fall_back_to_full_history(setting, value, monkeypatch):
    from nullion import chat_operator
    from nullion.runtime_store import RuntimeStore
    store = RuntimeStore()
    def forbidden(*args, **kwargs): raise AssertionError('Full history copied in finalization')
    monkeypatch.setattr(RuntimeStore, 'list_conversation_events', forbidden)
    if value is not None:
        store.add_conversation_event({'conversation_id':'telegram:77',
            'event_type':'conversation.session_settings', setting:value})
    getter = (chat_operator._session_chat_streaming_setting if setting=='chat_streaming_enabled'
              else chat_operator._session_thinking_display_setting)
    assert getter(SimpleNamespace(store=store), chat_id='77') is value


def registry(effect='read', approval=False):
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.plugins.browser_plugin.browser_reads import PageReadBatch
    tools = ToolRegistry()
    tools.register(ToolSpec('reader', 'Read independent pages', ToolRiskLevel.LOW,
        ToolSideEffectClass(effect), approval, 30, input_schema=PageReadBatch.model_json_schema(),
        independent_batch=('pages', 'independent_pages'), batch_item_required_fields=('url',)),
        lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'completed', {}))
    return tools


def calls():
    return [{'id':str(i), 'type':'tool_use', 'name':'reader',
             'input':{'pages':[{'url':f'https://example.test/{i}'}], 'independent_pages':True}}
            for i in range(2)]


def test_explicit_independent_model_plan_is_batched_without_mutating_it():
    from nullion.tool_batching import coalesce_independent_reads
    original = calls(); before = deepcopy(original)
    combined, plans = coalesce_independent_reads(original, registry())
    assert original == before and len(combined) == 1
    assert combined[0]['input']['pages'] == [row['input']['pages'][0] for row in before]
    assert plans == [{'tool_name':'reader', 'model_tool_use_ids':['0','1'],
                      'executed_tool_use_id':'0', 'item_count':2}]


@pytest.mark.parametrize('case', ['dependent', 'current_tab', 'session', 'invalid', 'write', 'approval', 'intervening'])
def test_batching_keeps_dependencies_permissions_and_invalid_calls_separate(case):
    from nullion.tool_batching import coalesce_independent_reads
    content = calls()
    if case=='dependent': content[1]['input']['independent_pages'] = False
    if case=='current_tab': content[1]['input']['pages'] = [{}]
    if case=='session': content[1]['input']['session_id'] = 'other-task'
    if case=='invalid': content[1]['input']['pages'][0]['read_kind'] = 'mutation'
    if case=='intervening': content.insert(1, {'type':'text', 'text':'別の手順'})
    result, plans = coalesce_independent_reads(content, registry('write' if case=='write' else 'read', case=='approval'))
    assert result == content and not plans


def test_batching_respects_declared_array_limit():
    from nullion.tool_batching import coalesce_independent_reads
    content = calls() * 0
    for i in range(8):
        row = calls()[0]; row['id'] = str(i); content.append(row)
    combined, plans = coalesce_independent_reads(content, registry())
    assert [len(row['input']['pages']) for row in combined] == [6,2]
    assert sum(row['item_count'] for row in plans) == 8


def test_incremental_command_receipt_preserves_ids_without_a_chat_head(tmp_path):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import checkpoint_runtime_conversation_events, load_runtime_store
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_conversation_ingress_id('telegram:77','input-1')
    runtime.store.add_committed_idempotency_key('telegram:77','action-1')
    checkpoint_runtime_conversation_events(runtime.store,runtime.checkpoint_path,conversation_id='telegram:77')
    restored=load_runtime_store(runtime.checkpoint_path)
    assert restored.has_conversation_ingress_id('telegram:77','input-1')
    assert restored.has_committed_idempotency_key('telegram:77','action-1')


def test_adapter_rejected_read_plan_is_not_coalesced():
    from nullion.tool_batching import coalesce_independent_reads
    tools=registry();tools.get_spec('reader').independent_batch_validator=lambda *args:False
    content=calls()
    assert coalesce_independent_reads(content,tools)==(content,[])


@pytest.mark.parametrize('effect,approval,artifacts,compact', [
    ('read',False,(),True), ('write',False,(),False),
    ('read',True,(),False), ('read',False,('.png',),False),
])
def test_initial_registered_read_scope_uses_metadata_for_compact_context(effect,approval,artifacts,compact):
    from nullion.chat_operator import _turn_tool_scope_uses_compact_delivery_contract
    tools=registry(effect,approval)
    tools.get_spec('reader').produced_artifact_extensions=artifacts
    tools.turn_tool_scope_decision=SimpleNamespace(requested_tool_names=('reader',))
    assert _turn_tool_scope_uses_compact_delivery_contract(tools) is compact

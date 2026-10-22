"""A discovered continuation cannot alter the original request's write budget."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from nullion.recovery_contract import RequestRecoveryContract
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass
from nullion.turn_context_policy import (ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision,
    _planner_materialized_registry)


def fixture_registry(effect=ToolSideEffectClass.WRITE):
    registry = ToolRegistry()
    writes = []
    for name, kind in [('catalog',ToolSideEffectClass.READ), ('detail',ToolSideEffectClass.READ),
                       ('change',effect), ('terminal_exec',ToolSideEffectClass.DANGEROUS_EXEC)]:
        def invoke(inv):
            if inv.tool_name == 'change': writes.append(inv.arguments)
            return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':17})
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,kind,False,2,
            continuation_tools=('detail','change') if name=='catalog' else ()),invoke)
    return registry,writes


@pytest.mark.parametrize('effect', [ToolSideEffectClass.WRITE, ToolSideEffectClass.EXTERNAL_WRITE,
                                  ToolSideEffectClass.ACCOUNT_WRITE])
@pytest.mark.parametrize('prompt', ['状態は？', '¿Qué novedades hay?', 'Any update?'])
def test_recovery_discovery_keeps_reads_but_not_unplanned_actions(effect,prompt):
    base,writes=fixture_registry(effect)
    original=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(current_user_message=prompt),
        tool_scope_decision=TurnToolScopeDecision(requested_tool_names=('catalog',),valid=True))
    contract=RequestRecoveryContract.from_registry(original)
    widened=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(),tool_scope_decision=replace(
        original.turn_tool_scope_decision,requested_tool_names=('catalog','detail','change'),required_tool_names=('change',)))
    recovered=contract.bind(widened)
    assert recovered.can_invoke_tool('detail')
    assert recovered.invoke(ToolInvocation('read','detail','workspace:sample',{})).output['value']==17
    result=recovered.invoke(ToolInvocation('bad','change','workspace:sample',{'value':4}))
    assert result.status=='denied' and result.output['reason']=='turn_effect_not_authorized'
    assert not writes and not recovered.can_invoke_tool('change')
    assert 'change' not in recovered.turn_tool_scope_decision.required_tool_names
    assert all('change' not in item.get('continuation_tools',[]) for item in recovered.list_tool_definitions())
    assert recovered.can_invoke_tool('terminal_exec')


def test_action_declared_before_discovery_survives_recovery_and_rebinding():
    base,writes=fixture_registry()
    original=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('catalog','change'),required_tool_names=('change',),valid=True))
    contract=RequestRecoveryContract.from_registry(original)
    recovered=contract.bind(ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('detail','change'),valid=True)))
    result=recovered.invoke(ToolInvocation('yes','change','workspace:sample',{'value':4}))
    assert result.status=='completed' and writes==[{'value':4}]
    assert recovered.turn_tool_scope_decision.required_tool_names==('change',)
    assert contract.bind(recovered) is recovered


def test_planner_preserves_all_runtime_contract_fields_and_current_evidence():
    base,_=fixture_registry()
    evidence=TurnToolEvidence(current_user_message='1', numbered_option_selected=True)
    original=TurnToolScopeDecision(valid=True,scheduler_action='mutate',scheduler_effect_actions=('inspect',),
        requested_outcome='generated_media',excluded_artifact_extensions=('.pdf',),
        source_artifact_retrieval_only=True,source_artifact_delivery_requested=True,
        source_artifact_request_evidence='1')
    materialized=_planner_materialized_registry(base,evidence=evidence,decision=original)
    actual=materialized.turn_tool_scope_decision
    assert actual.requested_outcome==original.requested_outcome
    assert actual.allowed_scheduler_actions==('inspect',)
    assert actual.source_artifact_retrieval_only is True
    assert materialized._evidence.current_user_message=='1'


def test_completed_scope_reconstruction_does_not_invent_account_consent():
    from nullion.chat_operator import _turn_scope_decision_from_completed_scope_results
    result=ToolResult('scope','request_tool_scope','completed',{'scope_requested':True,
        'available_tools':['email_read'],'capabilities':['connector'], 'connector_app_ids':['opaque-account'],
        'connector_source_user_requested':False,'requested_outcome':'generated_media',
        'source_artifact_delivery_requested':True,'read_only_source_delivery':True,
        'source_artifact_request_evidence':'2','excluded_artifact_extensions':['.pdf']})
    decision=_turn_scope_decision_from_completed_scope_results([result],available_tool_names=['email_read'])
    assert not decision.connector_source_user_requested
    assert decision.requested_outcome=='generated_media'
    assert decision.source_artifact_retrieval_only
    assert decision.source_artifact_request_evidence=='2' and decision.excluded_artifact_extensions==('.pdf',)


def test_exec_remains_available_but_executor_denials_are_not_bypassed(tmp_path,monkeypatch):
    from nullion.tools import ToolExecutor
    from test_browser_cleanup_and_delete_review import terminal_setup
    runtime,base,backend=terminal_setup(tmp_path,monkeypatch)
    original=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('file_read',),valid=True))
    recovered=RequestRecoveryContract.from_registry(original).bind(base)
    assert recovered.can_invoke_tool('terminal_exec')
    invocation=ToolInvocation('exec','terminal_exec','web:review',{'command':'pwd'})
    denied=ToolExecutor(store=runtime.store,registry=recovered,denied_tool_names=['terminal_exec']).invoke(invocation)
    assert denied.status=='denied' and denied.output['reason']=='capability_denied'
    assert not backend.calls
    result=ToolExecutor(store=runtime.store,registry=recovered).invoke(invocation)
    assert result.status=='completed' and backend.calls


def test_account_read_recovery_cannot_add_a_different_source_family():
    base=ToolRegistry()
    for name,tags in [('source_read',('connector','account_read','source_alpha')),
                      ('source_search',('connector','account_read','source_alpha')),
                      ('other_read',('connector','account_read','source_beta'))]:
        base.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,capability_tags=tags),
            lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{}))
    base.turn_tool_scope_decision=TurnToolScopeDecision(
        skill_pack_action='connector',connector_source_user_requested=True,requested_tool_names=('source_read',),valid=True)
    recovered=RequestRecoveryContract.from_registry(base).bind(base)
    assert recovered.can_invoke_tool('source_search')
    assert not recovered.can_invoke_tool('other_read')


def test_read_only_planner_does_not_implicitly_request_artifact_writes():
    base,_=fixture_registry()
    original=TurnToolScopeDecision(valid=True,requested_tool_names=('catalog',))
    materialized=_planner_materialized_registry(base,evidence=TurnToolEvidence(),decision=original)
    assert materialized.turn_tool_scope_decision.requested_tool_names==('catalog',)
    assert not materialized.can_invoke_tool('change')


def test_core_exec_discovery_is_not_a_terminal_work_requirement():
    from nullion.chat_operator import _turn_scope_decision_from_completed_scope_results
    result=ToolResult('scope','request_tool_scope','completed',{'scope_requested':True,
        'available_tools':['local_tool_search','local_tool_run','terminal_exec'],
        'capabilities':['local_tools'],'explicit_tool_names':[],'required_tool_names':[]})
    decision=_turn_scope_decision_from_completed_scope_results([result],available_tool_names=result.output['available_tools'])
    assert decision.requested_tool_names==('local_tool_search','local_tool_run')
    assert 'terminal_exec' not in decision.required_tool_names


def test_nested_recovery_filters_cannot_unwrap_action_authority():
    from nullion.agent_orchestrator import _block_tools_for_recovery,_focus_tools_for_completion_recovery
    base,_=fixture_registry()
    original=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('catalog',),valid=True))
    contract=RequestRecoveryContract.from_registry(original)
    blocked=contract.bind(_block_tools_for_recovery(base,{'catalog'}))
    again=_block_tools_for_recovery(blocked,{'detail'})
    assert not again.can_invoke_tool('change') and not again.can_invoke_tool('catalog')
    focused=contract.bind(_focus_tools_for_completion_recovery(base,{'catalog','change'}))
    again=_focus_tools_for_completion_recovery(focused,{'catalog','change'})
    assert not again.can_invoke_tool('change')
    assert again.can_invoke_tool('catalog')


def test_rebuilt_scope_cannot_turn_an_existing_file_edit_into_artifact_delivery():
    base,_=fixture_registry()
    original=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('catalog','change'),required_tool_names=('change',),valid=True))
    contract=RequestRecoveryContract.from_registry(original)
    rebuilt=ScopedTurnToolRegistry(contract.bind(base),evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('change',),requested_artifact_extensions=('.txt',),valid=True))
    assert rebuilt.turn_tool_scope_decision.requested_artifact_extensions==()
    assert rebuilt.turn_tool_scope_decision.required_tool_names==('change',)
    assert rebuilt.can_invoke_tool('change')


@pytest.mark.parametrize('name',['file_write','file_patch'])
def test_file_action_identity_is_not_an_attachment_requirement(name):
    from nullion.response_fulfillment_contract import artifact_paths_from_tool_results
    output={'path':'/workspace/files/note.txt','action_receipt':{'type':'action_receipt','object_type':'file'}}
    action=ToolResult('edit',name,'completed',output)
    assert artifact_paths_from_tool_results([action])==[]
    scope=ToolResult('scope','request_tool_scope','completed',{'artifact_extensions':['.txt']})
    assert artifact_paths_from_tool_results([scope,action])==['/workspace/files/note.txt']
    explicit=replace(action,output={**output,'artifact_path':'/workspace/files/note.txt'})
    assert artifact_paths_from_tool_results([explicit])==['/workspace/files/note.txt']


def test_read_source_suffix_cannot_create_an_output_contract():
    from nullion.response_fulfillment_contract import artifact_paths_from_tool_results,scoped_artifact_extensions_from_tool_results
    scope=ToolResult('scope','request_tool_scope','completed',{'artifact_extensions':[]})
    read=ToolResult('read','file_read','completed',{'path':'/workspace/files/note.txt','content':'17'})
    edit=ToolResult('edit','file_patch','completed',{'path':'/workspace/files/note.txt',
        'action_receipt':{'type':'action_receipt','object_type':'file'}})
    assert scoped_artifact_extensions_from_tool_results([scope,read,edit])==()
    assert artifact_paths_from_tool_results([scope,read,edit])==[]
    report=ToolResult('report','pdf_create','completed',{'path':'/workspace/artifacts/report.pdf'})
    assert scoped_artifact_extensions_from_tool_results([scope,read,report])==('.pdf',)
    assert artifact_paths_from_tool_results([scope,read,report])==['/workspace/artifacts/report.pdf']


def test_file_discovery_and_library_discovery_do_not_select_new_writes(tmp_path):
    from nullion.tools import create_default_tool_registry
    base=create_default_tool_registry(workspace_root=tmp_path)
    current=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence())
    read,current=current.apply_scope_request(ToolInvocation('read','request_tool_scope','web:scope',
        {'capabilities':['local_files'],'tool_names':['file_read']}))
    assert read.status=='completed'
    assert not current.can_invoke_tool('file_patch') and not current.can_invoke_tool('file_write')
    library,current=current.apply_scope_request(ToolInvocation('library','request_tool_scope','web:scope',
        {'capabilities':['local_tools']}))
    assert library.status=='completed' and current.can_invoke_tool('local_tool_read')
    assert not current.can_invoke_tool('local_tool_save')
    save,current=current.apply_scope_request(ToolInvocation('save','request_tool_scope','web:scope',
        {'capabilities':['local_tools'],'tool_names':['local_tool_save']}))
    assert save.status=='completed' and current.can_invoke_tool('local_tool_save')


def test_history_reference_does_not_mint_a_current_request_plan():
    import json
    from nullion.chat_operator import _augment_tool_registry_from_structured_tool_context
    base,_=fixture_registry()
    evidence=TurnToolEvidence(context_linked=True)
    current=ScopedTurnToolRegistry(base,evidence=evidence)
    context='Historical, timestamped tool outcomes from this conversation:\n'+json.dumps([
        {'tool_results':[{'tool_name':'catalog','status':'completed','output':{
            'continuation_tools':['detail','change']}}]}])
    widened=_augment_tool_registry_from_structured_tool_context(current,base_registry=base,
        evidence=evidence,context=context)
    assert widened.can_invoke_tool('request_tool_scope') and widened.can_invoke_tool('terminal_exec')
    assert not widened.can_invoke_tool('change')
    assert not widened.turn_tool_scope_decision.valid
    assert RequestRecoveryContract.from_registry(widened) is None


def test_existing_file_write_preserves_target_and_delivery_is_explicit(tmp_path,monkeypatch):
    from nullion.tools import ToolExecutor
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    from nullion.response_fulfillment_contract import artifact_paths_from_tool_results
    from test_browser_cleanup_and_delete_review import terminal_setup
    runtime,base,_=terminal_setup(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'home'/'workspaces'))
    roots=workspace_storage_roots_for_principal('web:review')
    target=roots.files/'note.txt';target.write_text('17')
    executor=ToolExecutor(store=runtime.store,registry=base)
    edited=executor.invoke(ToolInvocation('edit','file_write','web:review',{'path':str(target),'content':'23'}))
    assert edited.status=='completed' and edited.output['path']==str(target)
    assert target.read_text()=='23' and not (roots.artifacts/'note.txt').exists()
    assert artifact_paths_from_tool_results([edited])==[]
    delivered=executor.invoke(ToolInvocation('deliver','file_write','web:review',{'path':str(target),'content':'29'},
        flow_context={'requested_artifact_extensions':('.txt',)}))
    assert delivered.status=='completed' and delivered.output['path']==str(target), delivered
    from pathlib import Path
    copy=Path(delivered.output['artifact_path'])
    assert copy!=target and copy.parent==roots.artifacts and copy.read_bytes()==target.read_bytes()==b'29'
    assert artifact_paths_from_tool_results([delivered])==[str(copy)]


def test_tool_turns_checkpoint_bounded_history_without_full_store_scan(tmp_path,monkeypatch):
    from nullion.chat_operator import _should_checkpoint_conversation_events_after_chat_turn
    from nullion.runtime import bootstrap_persistent_runtime
    assert _should_checkpoint_conversation_events_after_chat_turn(tool_results=[ToolResult('r','file_read','completed',{})],artifact_count=0,suspended_for_approval=False)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    for i in range(140):
        runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'web:durable','user_message':str(i),'assistant_reply':str(i),'created_at':f'2026-10-19T01:{i//60:02d}:{i%60:02d}+00:00'})
    with monkeypatch.context() as bounded:
        bounded.setattr(type(runtime.store),'list_conversation_events',lambda *a,**k:(_ for _ in ()).throw(AssertionError('unbounded history copy')))
        runtime.checkpoint_conversation_events(limit=128)
    restored=bootstrap_persistent_runtime(runtime.checkpoint_path)
    saved=restored.store.list_conversation_events('web:durable')
    assert len(saved)==128 and saved[-1]['assistant_reply']=='139'

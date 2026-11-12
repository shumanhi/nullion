from dataclasses import replace
import pytest
from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolInvocation,ToolResult
from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision

@pytest.mark.parametrize('label',['Find the filename','Busca el nombre','ファイル名を検索'])
@pytest.mark.parametrize('new_artifact',[False,True])
@pytest.mark.parametrize('explicit_formats',[False,True])
def test_current_read_scope_retires_prior_write_obligations_only_without_new_artifact(label,new_artifact,explicit_formats):
    registry=ToolRegistry()
    for name,effect in [('file_search',ToolSideEffectClass.READ),('file_read',ToolSideEffectClass.READ),('file_write',ToolSideEffectClass.WRITE)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5),lambda i:ToolResult(i.invocation_id,i.tool_name,'completed',{}))
    decision=TurnToolScopeDecision(valid=True,requested_tool_names=('file_write','file_read'),required_tool_names=('file_write','file_read'),requested_artifact_extensions=('.csv',))
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message=label,context_linked=True),tool_scope_decision=decision)
    arguments={'capabilities':['local_files'],'tool_names':['file_search']+(['file_write'] if new_artifact else []),'required_tool_names':['file_search']+(['file_write'] if new_artifact else []),'source_artifact_delivery_requested':False,**({'artifact_extensions':['.csv'] if new_artifact else []} if explicit_formats or new_artifact else {})}
    result,widened=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:lookup',arguments))
    assert result.status=='completed',result
    required=widened.turn_tool_scope_decision.required_tool_names
    assert ('file_write' in required) is new_artifact
    if not new_artifact:
        assert required==('file_search',)
        assert set(result.output['superseded_unselected_required_tool_names'])=={'file_write','file_read'}


@pytest.mark.parametrize('filename',['cedar.csv','informe.csv','記録.csv'])
def test_verified_filename_reply_keeps_its_shape(filename):
    from nullion.chat_operator import _local_discovery_results_reply
    assert _local_discovery_results_reply(filename,('/workspace/'+filename,))==filename

@pytest.mark.parametrize('surface',['agent','chat','web','fulfillment'])
def test_current_read_plan_replaces_old_filename_format_guess(surface,tmp_path):
    from nullion.source_selection import current_read_only_local_scope
    scope=ToolResult('scope','request_tool_scope','completed',{'read_only_local_scope':True,'artifact_extensions':[],'source_artifact_delivery_requested':False,'superseded_unselected_required_tool_names':['file_write']})
    lookup=ToolResult('lookup','file_search','completed',{'matches':['/workspace/cedar.csv']})
    results=[scope,lookup]
    assert current_read_only_local_scope(results)
    if surface=='agent':
        from nullion.agent_orchestrator import _required_attachment_extensions_from_turn_state
        assert _required_attachment_extensions_from_turn_state({'tool_results':results,'tool_flow_context':{'artifact_extensions':['.csv']}})==()
    elif surface=='chat':
        from nullion.chat_operator import _merge_required_attachment_extensions
        assert _merge_required_attachment_extensions(('.csv',),results)==()
    elif surface=='web':
        from nullion.web_app import _web_merge_required_attachment_extensions
        assert _web_merge_required_attachment_extensions(('.csv',),results)==()
    else:
        from nullion.runtime import bootstrap_persistent_runtime
        from nullion.response_fulfillment_contract import evaluate_response_fulfillment
        runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
        decision=evaluate_response_fulfillment(store=runtime.store,conversation_id='web:lookup',user_message='cedar.csv',reply='cedar.csv',tool_results=results,required_attachment_extensions=('.csv',))
        assert decision.satisfied,decision
    produced=ToolResult('write','file_write','completed',{'artifact_path':'/workspace/cedar.csv'})
    assert not current_read_only_local_scope([produced,*results])

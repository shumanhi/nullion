"""Structured ID lookup and stale inventory recovery survive compaction."""
import pytest
from nullion.tools import ToolInvocation,ToolResult,_build_list_crons_handler
from nullion.scheduler_context_compaction import compact_list_crons_output_for_context
from nullion.tool_repair_contract import tool_repair_kind
from nullion.agent_orchestrator import _should_block_failed_tool_during_recovery

@pytest.mark.parametrize('name',['選択された対象','Objetivo seleccionado'])
def test_id_lookup_and_inventory_version_recovery_keep_exact_scope(tmp_path,monkeypatch,name):
    from nullion import crons
    monkeypatch.setenv('NULLION_HOME',str(tmp_path));monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db')
    monkeypatch.setattr('nullion.connections.workspace_id_for_principal',lambda _: 'workspace_admin')
    target=crons.add_cron(name,'0 0 1 1 *','Ready',enabled=False)
    crons.add_cron('Unrelated','0 0 1 1 *','Ready',enabled=False)
    handler=_build_list_crons_handler()
    args={'query':target.id,'limit':1}
    first=handler(ToolInvocation('first','list_crons','operator',args))
    assert [row['id'] for row in first.output['crons']]==[target.id]
    crons.toggle_cron(target.id,True)
    stale=handler(ToolInvocation('stale','list_crons','operator',{**args,'inventory_version':first.output['inventory_version']}))
    assert stale.status=='failed'
    compact=compact_list_crons_output_for_context(stale.output,user_message=target.id)
    assert compact['reason']=='inventory_changed' and compact['retryable'] is True
    recovery=ToolResult('stale','list_crons','failed',compact,stale.error)
    assert tool_repair_kind(recovery)=='correct_arguments' and not _should_block_failed_tool_during_recovery(recovery)
    corrected=compact['recovery']['arguments'];assert corrected['query']==target.id and corrected['limit']==1 and corrected['offset']==0
    result=handler(ToolInvocation('retry','list_crons','operator',corrected))
    assert result.status=='completed' and result.output['crons'][0]['enabled'] is True
    assert len(result.output['crons'])==1

@pytest.mark.parametrize('field,value',[('reason','permission_denied'),('retryable',False),
    ('terminal_user_action_required',True),('recovery',{'kind':'repair_precondition','required_tool_names':['owned_read']})])
def test_scheduler_compaction_preserves_control_facts_without_saved_task_prose(field,value):
    output={'crons':[{'id':'a','name':'Record','task':'Private task '*100}],field:value}
    compact=compact_list_crons_output_for_context(output)
    assert compact[field]==value and 'task' not in compact['crons'][0]

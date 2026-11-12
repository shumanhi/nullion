"""Scheduler read discovery follows native resource metadata, not user words."""
import pytest
from dataclasses import replace
from nullion.tools import ToolInvocation,ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
from nullion.turn_context_policy import scoped_turn_tool_registry,build_turn_tool_evidence,ScopedTurnToolRegistry


@pytest.mark.parametrize('prompt',['対象の状態は？','¿Cuál es el estado?','كيف حال المهمة؟'])
@pytest.mark.parametrize('capability,selected',[('live_tracking','list_live_trackers'),('scheduler_read','list_crons')])
def test_inventory_scope_exposes_related_reads_without_queries_or_mutations(prompt,capability,selected):
    registry=ToolRegistry();calls=[]
    for name in ['list_live_trackers','list_crons','list_reminders','toggle_cron']:
        effect=ToolSideEffectClass.WRITE if name=='toggle_cron' else ToolSideEffectClass.READ
        spec=ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,1,
            capability_tags=('scheduler_inventory',) if effect is ToolSideEffectClass.READ else ('scheduler',))
        def invoke(inv):calls.append(inv.tool_name);return ToolResult(inv.invocation_id,inv.tool_name,'completed',{})
        registry.register(spec,invoke)
    scoped=scoped_turn_tool_registry(registry,evidence=build_turn_tool_evidence(user_message=prompt,conversation_result=None),
        user_message=prompt)
    assert not calls
    result,scoped=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','operator',
        {'capabilities':[capability],'tool_names':[selected]}))
    assert result.status=='completed',result
    visible={row['name'] for row in scoped.list_tool_definitions()}
    assert {'list_live_trackers','list_crons','list_reminders'}<=visible
    assert 'toggle_cron' not in visible and not calls
    assert scoped.turn_tool_scope_decision.tracking_mode=='unspecified'
    denied,_=scoped.apply_scope_request(ToolInvocation('unrequested-write','request_tool_scope','operator',
        {'capabilities':['scheduler_mutate'],'tool_names':['toggle_cron'],'scheduler_action':'mutate'}))
    assert denied.status=='denied'
    # A subsequent explicit cron action must not be suppressed by a stale
    # live-mode guess made while merely inspecting an inventory.
    next_turn=ScopedTurnToolRegistry(registry,evidence=build_turn_tool_evidence(user_message=prompt,conversation_result=None),
        tool_scope_decision=replace(scoped.turn_tool_scope_decision,scheduler_action='mutate',scheduler_effect_actions=('inspect','mutate')))
    result,changed=next_turn.apply_scope_request(ToolInvocation('change','request_tool_scope','operator',
        {'capabilities':['scheduler_mutate'],'tool_names':['toggle_cron'],'scheduler_action':'mutate'}))
    assert result.status=='completed' and changed.can_invoke_tool('toggle_cron')
    assert not calls


def test_cron_inventory_scope_survives_context_compaction():
    from nullion.scheduler_context_compaction import compact_list_crons_output_for_context
    scope={'scheduler_kind':'cron','coverage':'single_kind_only'}
    assert compact_list_crons_output_for_context({'crons':[],'inventory_scope':scope})['inventory_scope']==scope


@pytest.mark.parametrize('action',['set','update','delete'])
def test_related_reminder_inventory_and_actions_preserve_workspace_boundaries(tmp_path,monkeypatch,action):
    from datetime import datetime,UTC,timedelta
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.reminders import ReminderRecord
    from nullion.tools import _build_list_reminders_handler,_build_set_reminder_handler,_build_update_reminder_handler,_build_delete_reminder_handler
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    own=ReminderRecord('own','workspace:alpha','Visible',datetime.now(UTC)+timedelta(days=1))
    other=ReminderRecord('other','workspace:beta','Private',datetime.now(UTC)+timedelta(days=1))
    runtime.store.add_reminder(own);runtime.store.add_reminder(other)
    result=_build_list_reminders_handler(runtime)(ToolInvocation('list','list_reminders','workspace:alpha',{}))
    assert [r['task_id'] for r in result.output['reminders']]==['own']
    assert result.output['inventory_scope']['scheduler_kind']=='reminder'
    handler={'set':_build_set_reminder_handler(runtime,default_chat_id=None),'update':_build_update_reminder_handler(runtime),'delete':_build_delete_reminder_handler(runtime)}[action]
    args=({'text':'No cross-workspace writes','chat_id':'workspace:beta','due_in_seconds':3600}
        if action=='set' else {'task_id':'other','text':'Changed','due_in_seconds':3600})
    denied=handler(ToolInvocation('write',action+'_reminder','workspace:alpha',args))
    assert denied.status=='denied' and denied.output['reason']=='workspace_access_denied'
    assert runtime.store.get_reminder('other')==other and len(runtime.store.list_reminders())==2


@pytest.mark.parametrize("name", ["記録", "Registro", "Record"])
def test_scheduler_inventory_projections_preserve_page_index_and_scope(name):
    from nullion.scheduler_context_compaction import compact_list_crons_output_for_context
    from nullion.agent_orchestrator import (
        _compact_tool_output_for_model_context, _compact_tool_output_for_repair,
    )
    scope = {"scheduler_kind": "cron", "coverage": "single_kind_only"}
    rows = [{"id": str(i), "name": f"{name} {i}", "selection_index": i + 1,
             "enabled": False, "schedule_description": "09:00"} for i in range(69)]
    output = compact_list_crons_output_for_context({"crons": rows, "offset": 0,
        "limit": 100, "matched_count": 69, "has_more": False, "next_offset": None,
        "inventory_scope": scope})
    for project in (_compact_tool_output_for_model_context, _compact_tool_output_for_repair):
        projected = project("list_crons", output)
        assert projected["cron_count"] == 69
        assert len(projected["crons"]) == 12
        assert projected["inventory_scope"] == scope
        assert projected["inventory_index"] == output["inventory_index"]
        assert projected["inventory_index_guidance"] == output["inventory_index_guidance"]
        assert projected["inventory_index_complete"] is True
        assert projected["source_page_has_more"] is False
        assert projected["context_sample_only"] is True
        assert projected["next_offset"] == 0

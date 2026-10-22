from __future__ import annotations
import pytest
from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
from nullion.agent_orchestrator import _scheduler_read_completion_text
from nullion.scheduler_context_compaction import compact_list_crons_output_for_context

@pytest.mark.parametrize('action', ['', 'inspect', 'run', 'mutate'])
def test_core_execution_survives_scoping(action):
    registry = ToolRegistry()
    registry.register(ToolSpec(name='terminal_exec', description='Core execution',
        risk_level=ToolRiskLevel.HIGH, side_effect_class=ToolSideEffectClass.WRITE,
        requires_approval=True, timeout_seconds=5), lambda invocation: None)
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(scheduler_action=action, valid=True))
    assert scoped.can_invoke_tool('terminal_exec')
    assert scoped.get_spec('terminal_exec').requires_approval
    assert 'terminal_exec' in {s.name for s in scoped.list_specs()}
    assert 'terminal_exec' in {d['name'] for d in scoped.list_tool_definitions()}

@pytest.mark.parametrize('prompt', ['Are you still tracking my flight? How often?', '¿Sigues vigilando mi vuelo?', 'フライトの監視状況は？'])
def test_inventory_does_not_terminate_question(prompt):
    result = ToolResult('read', 'list_crons', 'completed', {'crons': [{'name':'Fare tracker'}]})
    assert _scheduler_read_completion_text(result, tool_registry=None, tool_results=[result], user_message=prompt) is None

def test_health_survives_model_context_compaction():
    health = {'state':'incomplete', 'last_success_at':None, 'consecutive_failures':2}
    output = compact_list_crons_output_for_context({'crons':[{'id':'fare', 'name':'Fare tracker',
        'schedule_description':'Every 30 minutes', 'tracker_status':health, 'last_run':'2026-10-15T01:00:00Z'}]})
    assert output['crons'][0]['tracker_status'] == health
    assert output['crons'][0]['last_run'] == '2026-10-15T01:00:00Z'

@pytest.mark.parametrize('query', ['STRASSE', '航空券'])
def test_cron_search_uses_structured_query_and_preserves_workspace(tmp_path, monkeypatch, query):
    from nullion import crons
    from nullion.tools import _build_list_crons_handler, ToolInvocation
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path/'runtime.db')
    crons.add_cron('Straße 航空券', '*/30 * * * *', 'Watch fares', workspace_id='workspace_admin')
    crons.add_cron('Unrelated', '0 8 * * *', 'Other task', workspace_id='workspace_admin')
    crons.add_cron('Straße 航空券 private', '0 8 * * *', 'Private', workspace_id='workspace_other')
    result = _build_list_crons_handler()(ToolInvocation('find', 'list_crons', 'web:admin',
        {'query':query, 'workspace_id':'workspace_admin'}))
    assert [row['name'] for row in result.output['crons']] == ['Straße 航空券']

def test_final_answer_about_named_tracker_is_not_replaced_by_inventory():
    from nullion.response_sanitizer import sanitize_user_visible_reply
    text = 'Your Condition Alpha is enabled and checks every 30 minutes. No successful check is recorded yet.'
    result = ToolResult('read','list_crons','completed',{'crons':[{'name':'Condition Alpha',
        'enabled':True,'schedule_description':'Every 30 minutes'}], 'message':'Here are your 1 crons: Condition Alpha'})
    assert sanitize_user_visible_reply(user_message="Are you still tracking my flight? How often?", reply=text, tool_results=[result]) == text

def test_core_execution_discovery_respects_delegate_restriction():
    class RestrictedRegistry(ToolRegistry):
        def can_invoke_tool(self, name):
            return False
    registry = RestrictedRegistry()
    registry.register(ToolSpec(name='terminal_exec',description='Execution',risk_level=ToolRiskLevel.HIGH,
        side_effect_class=ToolSideEffectClass.WRITE,requires_approval=True,timeout_seconds=1),lambda inv:None)
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence())
    assert not scoped.can_invoke_tool('terminal_exec')
    assert 'terminal_exec' not in {spec.name for spec in scoped.list_specs()}


def test_empty_search_does_not_claim_no_jobs_exist(tmp_path,monkeypatch):
    from nullion import crons
    from nullion.tools import _build_list_crons_handler,ToolInvocation
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db')
    crons.add_cron('条件 A','*/30 * * * *','Observe condition A')
    result=_build_list_crons_handler()(ToolInvocation('lookup','list_crons','web:admin',{'query':'条件 B'}))
    assert result.output['crons']==[] and result.output['total_count']==1
    assert result.output['query']=='条件 b'
    assert result.output['message']=='No scheduled tasks match this search.'
    compact=compact_list_crons_output_for_context(result.output)
    assert compact['query']=='条件 b' and compact['total_cron_count']==1


@pytest.mark.parametrize('tool_name,field',[('list_crons','crons'),('list_reminders','reminders')])
@pytest.mark.parametrize('backend_failure',[False,True])
def test_scope_read_retry_keeps_untrusted_evidence_and_survives_backend_failure(monkeypatch,tool_name,field,backend_failure):
    import json
    from types import SimpleNamespace
    from nullion import chat_operator
    from nullion.tools import ToolInvocation
    sentinel='UNTRUSTED_SCHEDULER_ROW'
    rows=[{'id':'neutral','name':sentinel,'enabled':True,'text':sentinel}]
    registry=ToolRegistry()
    registry.register(ToolSpec(name=tool_name,description='Read scheduler',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=1,
        capability_tags=('scheduler',)),lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{field:rows}))
    initial=SimpleNamespace(final_text='I requested read-only scheduler access, but the actual scheduled-task listing tool was not exposed.',tool_results=[],artifacts=[],suspended_for_approval=False)
    monkeypatch.setattr(chat_operator,'build_turn_tool_scope_decision',lambda **kw:TurnToolScopeDecision(valid=False))
    calls=[]
    def run_turn(history,scoped):
        calls.append(history)
        if len(calls)==1:
            return SimpleNamespace(final_text='Scope updated.',artifacts=[],suspended_for_approval=False,
                tool_results=[ToolResult('scope','request_tool_scope','completed',{'scope_requested':True,
                    'capabilities':['scheduler_read'],'scheduler_action':'inspect','available_tools':[tool_name],
                    'required_tool_names':[]})])
        assert all(sentinel not in str(m) for m in history if m['role']=='system')
        assert history[-1]['role']=='user'
        evidence=json.loads(history[-1]['content'][0]['text'])['scheduler_evidence'][0]
        assert evidence['tool_name']==tool_name and evidence['output'][field][0]['name']==sentinel
        if backend_failure:raise chat_operator.ChatBackendUnavailableError('provider unavailable')
        return SimpleNamespace(final_text='The selected item is enabled.',tool_results=[],artifacts=[],suspended_for_approval=False)
    result,scoped,retried=chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial,base_history=[],active_tool_registry=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence()),
        base_tool_registry=registry,model_client=object(),user_message='対象の状態は？',evidence=TurnToolEvidence(),
        run_turn=run_turn,principal_id='web:admin')
    assert retried and len(calls)==2
    assert [x.tool_name for x in result.tool_results]==['request_tool_scope',tool_name]
    if backend_failure:
        assert "couldn't finish" in result.final_text and sentinel not in result.final_text
    else:assert result.final_text=='The selected item is enabled.'


def test_empty_reminder_read_does_not_replace_relevant_tracker_answer():
    from nullion.response_sanitizer import sanitize_user_visible_reply
    reply='Yes, it checks every 30 minutes.'
    results=[ToolResult('a','list_crons','completed',{'crons':[{'name':'条件 A'},{'name':'条件 B'}],
        'message':'1. 条件 A\n2. 条件 B'}),ToolResult('b','list_reminders','completed',{'reminders':[],'count':0})]
    assert sanitize_user_visible_reply(user_message='追跡状況は？',reply=reply,tool_results=results)==reply

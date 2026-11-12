import pytest
from nullion.agent_orchestrator import _retry_unmet_readable_requirements as recover,_CompletionReviewDecision as Review,_CompletionReviewMaterialRequirement as Coverage,_CompletionRequirementContractRow as Row
from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult

@pytest.mark.parametrize('kind',['fact','comparison_side'])
def test_recoverable_missing_read_evidence_keeps_the_same_request(kind):
    registry=ToolRegistry();registry.register(ToolSpec('source_read','Read current source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),lambda inv:None)
    decision=Review('blocked',('Current value is not yet verified',),all_material_requirements_enumerated=True,material_requirements=(Coverage('r','Current value',kind,False,False),))
    result=recover(decision,requirement_contract=(Row('r','Current value',kind,('source_read',)),),tool_results=[],registry=registry)
    assert result.disposition=='retry' and result.retry_tool_names==('source_read',) and result.material_requirements==decision.material_requirements


def test_prior_partial_error_does_not_override_a_later_successful_read():
    registry=ToolRegistry();registry.register(ToolSpec('source_read','Read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),lambda inv:None)
    decision=Review('blocked',('A new value is missing',),all_material_requirements_enumerated=True,material_requirements=(Coverage('r','New value','fact',False,False),))
    results=[ToolResult('partial','source_read','completed',{'errors':[{'subject':'old','error':'Unavailable'}]}),ToolResult('recovered','source_read','completed',{'value':37,'errors':[]})]
    assert recover(decision,requirement_contract=(Row('r','New value','fact',('source_read',)),),tool_results=results,registry=registry).disposition=='retry'


@pytest.mark.parametrize('kind',['fact','comparison_side','action','constraint','deliverable'])
@pytest.mark.parametrize('label',['Current source','Fuente actual','現在の情報源'])
def test_later_verified_source_adapter_can_prove_same_fact_without_changing_outcome(kind,label):
    from nullion.agent_orchestrator import _with_current_source_evidence_tools,_FocusedRecoveryToolRegistry
    registry=ToolRegistry();registry.register(ToolSpec('read_detail',label,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),lambda inv:None)
    registry.register(ToolSpec('writer','Write',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),lambda inv:None)
    focused=_FocusedRecoveryToolRegistry(registry,{'writer'})
    contract=(Row('r',label,kind,('discovery',)),)
    results=[ToolResult('source','read_detail','completed',{'rows':[{'id':'subject','value':37}], 'source_record_collections':[{'path':['rows'],'identity_fields':['id']}]})]
    expanded=_with_current_source_evidence_tools(contract,tool_results=results,registry=focused)
    assert expanded[0].requirement_id=='r' and expanded[0].requirement==label
    assert ('read_detail' in expanded[0].eligible_evidence_tool_names)==(kind in {'fact','comparison_side'})
    assert contract[0].eligible_evidence_tool_names==('discovery',)
    assert not focused.can_invoke_tool('read_detail')
    results[0]=ToolResult('write','writer','completed',results[0].output)
    assert _with_current_source_evidence_tools(contract,tool_results=results,registry=registry)==contract

@pytest.mark.parametrize('case',['action','constraint','deliverable','denied','not_callable','missing','error','no_coverage','terminal_stop','write_only','failed','partial_errors','error_map'])
def test_recovery_cannot_invent_work_or_bypass_a_boundary(case):
    registry=ToolRegistry();registry.register(ToolSpec('source_read','Read current source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),lambda inv:None)
    kind=case if case in {'action','constraint','deliverable'} else 'fact'
    if case=='write_only':
        registry=ToolRegistry();registry.register(ToolSpec('source_read','Write only',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),lambda inv:None)
    if case=='not_callable':registry.can_invoke_tool=lambda name:False
    if case=='missing':registry=ToolRegistry()
    results=[ToolResult('denied','source_read','denied',{})] if case=='denied' else []
    if case=='failed':results=[ToolResult('failed','source_read','failed',{},'Unavailable')]
    if case in {'partial_errors','error_map'}:results=[ToolResult('partial','source_read','completed',{'errors':[{'subject':'CCC','error':'Unavailable'}] if case=='partial_errors' else {'CCC':'Unavailable'}})]
    if case=='terminal_stop':results=[ToolResult('stop','source_read','completed',{'result_kind':'source_observations','report_status':'blocked','next_action':'report'})]
    decision=Review('blocked',('Unknown',),all_material_requirements_enumerated=case!='no_coverage',material_requirements=(Coverage('r','Current value',kind,False,False),),review_error='invalid_response' if case=='error' else None)
    assert recover(decision,requirement_contract=(Row('r','Current value',kind,('source_read',)),),tool_results=results,registry=registry)==decision

@pytest.mark.parametrize('language',['Información','情報','Information'])
def test_distinct_scope_transitions_continue_but_identical_contracts_stop(language):
    from nullion.agent_orchestrator import _scope_request_stalled_after_completed_tool as stalled
    done=ToolResult('done','file_write','completed',{'path':'report.txt'})
    local=ToolResult('scope1','request_tool_scope','completed',{'available_tools':['file_write'],'capabilities':['local_files'],'message':language})
    web=ToolResult('scope2','request_tool_scope','completed',{'available_tools':['web_search','file_write'],'capabilities':['web','local_files']})
    assert not stalled([done,local,web])
    equivalent=ToolResult('scope3','request_tool_scope','completed',{'available_tools':['file_write','web_search'],'capabilities':['local_files','web'],'message':'different display wording'})
    assert stalled([done,web,equivalent])

@pytest.mark.parametrize('status,available,expected',[
    ('completed',['lectura_exacta'],True),('failed',['lectura_exacta'],False),('completed',['unrelated'],False)])
def test_focus_extends_only_validated_exact_scope(status,available,expected):
    from nullion.agent_orchestrator import _FocusedRecoveryToolRegistry as Focus
    from nullion.tools import ToolInvocation
    class Delegate:
        def apply_scope_request(self,invocation):
            return ToolResult(invocation.invocation_id,'request_tool_scope',status,{'available_tools':available+['denied']}),self
        def get_spec(self,name):
            return ToolSpec(name,'read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2)
        def can_invoke_tool(self,name):return name!='denied'
    invocation=ToolInvocation(invocation_id='scope',tool_name='request_tool_scope',principal_id='user',arguments={'tool_names':['lectura_exacta','denied']},capsule_id='chat')
    result,focused=Focus(Delegate(),{'request_tool_scope'}).apply_scope_request(invocation)
    assert focused.can_invoke_tool('lectura_exacta') is expected
    assert not focused.can_invoke_tool('unrelated') and not focused.can_invoke_tool('denied')


@pytest.mark.parametrize('reader,setup',[('lecture','ouvrir'),('取得','移動')])
def test_completion_focus_keeps_declared_callable_read_prerequisites(reader,setup):
    from nullion.agent_orchestrator import _focus_tools_for_completion_recovery
    registry=ToolRegistry()
    registry.register(ToolSpec(reader,'Read current source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,
        observation_setup_tools=(setup,'account_change','missing')),lambda inv:None)
    registry.register(ToolSpec(setup,'Open selected source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),lambda inv:None)
    registry.register(ToolSpec('account_change','Change account',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),lambda inv:None)
    focused=_focus_tools_for_completion_recovery(registry,[reader])
    assert {r['name'] for r in focused.list_tool_definitions()}=={reader,setup}
    assert _focus_tools_for_completion_recovery(focused,[reader]).can_invoke_tool(setup)
    assert not focused.can_invoke_tool('account_change')

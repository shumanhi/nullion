"""Recovery receipts cannot escape their current request identity."""
import pytest
from nullion.agent_orchestrator import AgentOrchestrator,TurnContinuationEvidence
from nullion.chat_operator import _RevisionContinuationRegistry
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolRegistry,ToolResult,ToolSpec,ToolRiskLevel,ToolSideEffectClass
from nullion.turn_context_policy import TurnToolScopeDecision

@pytest.mark.parametrize('mismatch',[None,'conversation','principal','message'])
def test_receipts_only_satisfy_the_same_request(mismatch):
    store=RuntimeStore();registry=ToolRegistry()
    def forbidden(invocation):raise AssertionError('A completed read must not be executed again')
    registry.register(ToolSpec('file_read','Read reference',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),forbidden)
    registry.turn_tool_scope_decision=TurnToolScopeDecision(valid=True,confidence=1,
        requested_tool_names=('file_read',),required_tool_names=('file_read',))
    receipt=ToolResult('previous-read','file_read','completed',{'content':'Reference 42'})
    evidence=TurnContinuationEvidence(conversation_id='web:other' if mismatch=='conversation' else 'web:current',
        principal_id='other-principal' if mismatch=='principal' else 'web:current',parent_turn_ids=('current-turn',),
        tool_results=(receipt,),same_request_retry=True,user_message='unrelated request' if mismatch=='message' else '要求を完了。')
    observed=[]
    class Provider:
        def create(self,**kwargs):
            observed.append(str(kwargs.get('messages')))
            return {'content':[{'type':'text','text':'The reference is 42.'}],'stop_reason':'end_turn'}
    result=AgentOrchestrator(model_client=Provider()).run_turn(conversation_id='web:current',principal_id='web:current',
        user_message='要求を完了。',conversation_history=[],tool_registry=_RevisionContinuationRegistry(registry,evidence),
        policy_store=store,approval_store=store,max_iterations=8)
    assert any('Reference 42' in message for message in observed) is (mismatch is None), observed
    if mismatch is None:
        assert [r.invocation_id for r in result.tool_results]==['previous-read']
        assert "didn't run" not in result.final_text
    else:
        assert not result.tool_results
        assert result.response_fulfilled is False



@pytest.mark.parametrize('required',[True,False,None,'true','false',1])
def test_link_hint_requires_a_typed_obligation(required):
    from nullion.chat_operator import _turn_result_schema_followup_tool_names, _turn_result_structured_account_browser_followup_tool_names
    result=type('Result',(),{'tool_results':[ToolResult('r','email_read','completed',{'message':{
        'next_tools_for_links':['browser_navigate'],'link_followup_required':required}})]})()
    expected=('browser_navigate',) if required is True else ()
    assert _turn_result_schema_followup_tool_names(result)==expected
    assert _turn_result_structured_account_browser_followup_tool_names(result)==expected


def test_retained_model_evidence_is_bounded_and_excludes_scope_inventory():
    from nullion.agent_orchestrator import _continuation_model_evidence_message
    scope=ToolResult('scope','request_tool_scope','completed',{'inventory':'PRIVATE-INVENTORY'})
    values=[scope,*[ToolResult('r'+str(i),'file_read','completed',{'content':'\\"資料'*30000}) for i in range(40)]]
    message=_continuation_model_evidence_message(values)
    text=message['content'][0]['text']
    assert len(text)<25000 and 'PRIVATE-INVENTORY' not in text
    assert 'omitted_record_count": 16' in text and 'truncated' in text

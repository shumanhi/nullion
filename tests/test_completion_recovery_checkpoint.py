from nullion.agent_orchestrator import _completion_recovery_checkpoint
from nullion.tools import ToolResult


def test_plain_turn_does_not_scan_results_or_add_recovery_context():
    class Unreadable:
        def __iter__(self): raise AssertionError('Plain fast path scanned tools')
    assert _completion_recovery_checkpoint({'tool_results': Unreadable()}) is None


def test_checkpoint_counts_current_call_and_does_not_claim_navigation_is_evidence():
    def result(name, output, status='completed'):
        return ToolResult(name,name,status,output)
    state={'iterations':12,'max_iterations':20,'completion_recovery_iteration_limit':17,
        'completion_review_count':1,'completion_review_unresolved_requirements':['接続','acceso'],
        'artifacts':['report.html','report.html'],'tool_results':[
            result('browser_navigate',{'session_id':'a','secret':'omitted'}),
            result('browser_extract_text',{'session_id':'a','text':'do not copy private page text'}),
            result('browser_navigate',{'session_id':'a'}),
            result('browser_navigate',{'session_id':'b'}),
            result('browser_extract_text',{'session_id':'b'},status='failed')]}
    checkpoint=_completion_recovery_checkpoint(state)
    assert checkpoint=={'remaining_model_calls':5,'unresolved_outcomes':['接続','acceso'],
        'existing_artifact_paths':['report.html'],'browser_sessions':[
            {'session_id':'a','text_captured_since_navigation':False},
            {'session_id':'b','text_captured_since_navigation':False}]}
    state['max_iterations']=14
    assert _completion_recovery_checkpoint(state)['remaining_model_calls']==2

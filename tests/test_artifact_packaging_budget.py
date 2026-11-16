from copy import copy
import pytest


@pytest.mark.parametrize('iteration,allowed',[(14,True),(15,True),(16,True),(17,False),(13,False)])
def test_packaging_tail_has_three_rounds_and_cannot_restart(iteration,allowed):
    from nullion.agent_orchestrator import _FocusedRecoveryToolRegistry,_artifact_packaging_recovery_active
    state={'iterations':iteration,'artifact_packaging_start_iteration':14,
        'artifact_packaging_attempt_count':1,'artifact_packaging_selection_required':True,
        'artifact_packaging_request':{'linked_paths':['prepared.html']},
        'tool_registry':_FocusedRecoveryToolRegistry(None,{'request_tool_scope','archive_create','file_read','file_write'})}
    before=copy(state)
    assert _artifact_packaging_recovery_active(state) is allowed
    assert state==before


@pytest.mark.parametrize('invalid',['plain','unfocused','research','untyped','no_members','too_many_attempts','not_pending'])
def test_packaging_allowance_requires_typed_local_scope(invalid):
    from nullion.agent_orchestrator import _FocusedRecoveryToolRegistry,_artifact_packaging_recovery_active
    state={'iterations':14,'artifact_packaging_start_iteration':14,
        'artifact_packaging_attempt_count':1,'artifact_packaging_selection_required':True,
        'artifact_packaging_request':{'linked_paths':['prepared.html']},
        'tool_registry':_FocusedRecoveryToolRegistry(None,{'archive_create'})}
    if invalid=='plain':state={}
    if invalid=='unfocused':state['tool_registry']=None
    if invalid=='research':state['tool_registry']=_FocusedRecoveryToolRegistry(None,{'archive_create','web_search'})
    if invalid=='untyped':state['artifact_packaging_start_iteration']=True
    if invalid=='no_members':state['artifact_packaging_request']={'linked_paths':[]}
    if invalid=='too_many_attempts':state['artifact_packaging_attempt_count']=3
    if invalid=='not_pending':state['artifact_packaging_selection_required']=False
    assert not _artifact_packaging_recovery_active(state)

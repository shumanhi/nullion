"""Optional UI tokens never replace native approval/source ownership."""
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize('format',['db','json'])
@pytest.mark.parametrize('anchor',['token','scope','event'])
def test_missing_ui_token_uses_verified_native_source_not_later_head(tmp_path,format,anchor):
    from nullion.approvals import create_approval_request
    from nullion.chat_operator import _approval_resume_source,_remember_chat_turn
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolResult
    runtime=bootstrap_persistent_runtime(tmp_path/('runtime.'+format))
    source='web:owned'
    runtime.process_conversation_message(conversation_id=source,chat_id='owned',user_message='資料',turn_id='original',checkpoint=False)
    runtime.process_conversation_message(conversation_id='web:foreign',chat_id='foreign',user_message='Other',turn_id='foreign',checkpoint=False)
    approval=create_approval_request(source,'use_tool','opaque_tool',context={'trigger_flow':{'conversation_id':source,
        'browser_session_scope':'foreign' if anchor=='event' else 'original'}})
    runtime.store.add_approval_request(approval)
    if anchor=='event':
        _remember_chat_turn(runtime,chat_id=source,conversation_id=source,user_message='資料',assistant_reply='確認',
            conversation_turn_id='original',tool_results=[ToolResult('native','opaque_tool','denied',
                {'requires_approval':True,'approval_id':approval.approval_id})])
    runtime.process_conversation_message(conversation_id=source,chat_id='owned',user_message='Later unrelated request',turn_id='later',checkpoint=False)
    turn=SuspendedTurn(approval.approval_id,source,'owned','/chat 資料',None,None,datetime.now(UTC),
        resume_token={'conversation_turn_id':'original'} if anchor=='token' else None)
    runtime.store.add_suspended_turn(turn);runtime.checkpoint(force=True)
    restored=bootstrap_persistent_runtime(runtime.checkpoint_path)
    found=_approval_resume_source(restored,restored.store.get_approval_request(approval.approval_id),
        restored.store.get_suspended_turn(approval.approval_id))
    assert found.turn_id=='original' and found.original_turn_resolved is True
    assert found.kind=={'token':'native_token','scope':'native_scope','event':'native_approval_event'}[anchor]


def test_unknown_source_ownership_stops_before_claim_or_execution(tmp_path):
    from nullion.approvals import create_approval_request
    from nullion.approval_resume import approval_resume_claimed
    from nullion.chat_operator import resume_approved_telegram_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    approval=create_approval_request('web:owned','use_tool','opaque_tool',context={'trigger_flow':{'conversation_id':'web:owned'}})
    runtime.store.add_approval_request(approval)
    runtime.approve_approval_request(approval.approval_id,principal_id='web:owned',permissions=())
    runtime.store.add_suspended_turn(SuspendedTurn(approval.approval_id,'web:foreign','foreign','/chat 資料',None,None,
        datetime.now(UTC),messages_snapshot=[{'role':'user','content':[]}]))
    class Runner:
        def resume_turn(self,**kwargs):pytest.fail('Unknown source cannot execute')
    assert resume_approved_telegram_request(runtime,approval_id=approval.approval_id,chat_id='operator',agent_orchestrator=Runner()) is None
    assert not approval_resume_claimed(runtime,approval.approval_id)
    assert runtime.store.get_suspended_turn(approval.approval_id) is not None

from datetime import datetime
import pytest
from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.tools import ToolInvocation, ToolResult, _build_email_read_handler, _email_message_evidence_scope

MESSAGE = {'id':'mail-old','subject':'Scheduling confirmation', 'from':'Service <service@example.test>',
           'date':'Thu, 11 Jun 2026 15:34:57 -0700', 'snippet':'Your request was scheduled.',
           'body':'Amount: $3,000.00. Scheduled date: June 14, 2026.'}

@pytest.mark.parametrize('reply',[
    "I can't confirm a payment scheduled now. The $3,000 confirmation is for June 14, 2026, which is past.",
    'No puedo confirmar un pago pendiente ahora. El correo confirma $3,000 para el 14 de junio de 2026.',
    '現在の支払い予約は確認できません。過去の確認メールは2026年6月14日の3,000ドルの支払いです。',
    'No current status is established. If you want further verification, use the account portal.',
])
@pytest.mark.parametrize('empty_after_read',[False,True])
def test_mixed_search_evidence_never_replaces_answer_with_old_email_preview(reply, empty_after_read):
    positive=ToolResult('a','email_search','completed',{'results':[MESSAGE],'resultSizeEstimate':1})
    empty=ToolResult('b','email_search','completed',{'results':[],'resultSizeEstimate':0})
    read=ToolResult('c','email_read','completed',{'message':MESSAGE})
    results=[positive,read,empty] if empty_after_read else [positive,empty,read]
    metadata={}
    visible=sanitize_user_visible_reply(user_message='確認して',reply=reply,tool_results=results,presentation_metadata=metadata)
    assert reply in visible
    assert 'Preview:' not in visible and 'From:' not in visible
    assert metadata=={}

@pytest.mark.parametrize('date,expected',[
    ('Thu, 11 Jun 2026 15:34:57 -0700','2026-06-11T22:34:57+00:00'),
    ('2026-06-11T22:34:57+00:00','2026-06-11T22:34:57+00:00'),
    ('not-a-date',None),('',None),
])
def test_email_record_time_is_not_retrieval_or_live_account_state(date,expected):
    scope=_email_message_evidence_scope({'date':date})
    assert scope['kind']=='email_message'
    assert scope['message_sent_at']==expected
    assert datetime.fromisoformat(scope['retrieved_at']).tzinfo is not None
    assert scope['current_account_state_verified'] is False


def test_real_native_read_handler_preserves_body_and_exposes_evidence_scope():
    result=_build_email_read_handler(lambda message_id:dict(MESSAGE))(ToolInvocation(invocation_id='read',tool_name='email_read',arguments={'id':'mail-old'},principal_id='user'))
    assert result.status=='completed'
    assert result.output['message']['body']==MESSAGE['body']
    assert result.output['evidence_scope']['message_sent_at']=='2026-06-11T22:34:57+00:00'
    assert result.output['evidence_scope']['current_account_state_verified'] is False


def test_empty_only_search_retains_safe_empty_result_summary():
    result=ToolResult('empty','email_search','completed',{'results':[],'resultSizeEstimate':0})
    visible=sanitize_user_visible_reply(user_message='調べて',reply='',tool_results=[result])
    assert 'No matching messages found' in visible


@pytest.mark.parametrize('reply',[
    "I cannot confirm a current payment. No confirmation was found in the checked 90-day window.",
    'No puedo confirmar un pago pendiente. No hay confirmación en los últimos 90 días.',
    '現在の支払い予約は確認できません。直近90日間に確認メールは見つかりませんでした。',
])
def test_empty_only_search_preserves_qualified_status_answer(reply):
    result=ToolResult('empty','email_search','completed',{'results':[],'resultSizeEstimate':0})
    visible=sanitize_user_visible_reply(user_message='確認して',reply=reply,tool_results=[result])
    assert reply in visible
    assert 'No matching messages found' not in visible


def test_empty_search_has_no_impossible_body_read_continuation():
    from nullion.tools import _build_email_search_handler
    result=_build_email_search_handler(lambda query,limit:[])(ToolInvocation(
        invocation_id='search',tool_name='email_search',arguments={'query':'recent-window'},principal_id='user'))
    assert result.status=='completed' and result.output['results']==[]
    assert result.output['body_included'] is False
    assert 'next_tool_for_body' not in result.output and 'body_requires_tool' not in result.output


@pytest.mark.parametrize('prompt',['確認して','Comprueba la confirmación','تأكد من التأكيد'])
def test_email_preview_is_typed_unfulfilled_evidence(prompt):
    from nullion.response_sanitizer import _email_read_summary
    read=ToolResult('read','email_read','completed',{'message':MESSAGE})
    metadata={}
    sanitize_user_visible_reply(user_message=prompt,reply=_email_read_summary(read.output),tool_results=[read],presentation_metadata=metadata)
    assert metadata['kind'] in {'source_preview','source_summary'}
    assert metadata['fulfillment_satisfied'] is False


def test_direct_terminal_shortcut_cannot_record_email_preview_as_verified():
    from nullion.agent_orchestrator import _complete_agent_turn
    from nullion.response_sanitizer import _email_read_summary
    from nullion.tools import ToolRegistry
    read=ToolResult('read','email_read','completed',{'message':MESSAGE})
    state={'tool_results':[read],'tool_registry':ToolRegistry(),'user_message':'確認して','principal_id':'user',
           'conversation_id':'telegram:1','cleanup_done':True}
    result=_complete_agent_turn(state,final_text=_email_read_summary(read.output),response_fulfilled=True)['result']
    assert result.response_fulfilled is False
    assert result.response_presentation['kind']=='unverified_email_answer'
    assert 'Preview:' not in result.final_text and 'From:' not in result.final_text
    assert result.completion_failure_reply==result.final_text

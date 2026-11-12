import json
from types import SimpleNamespace

import pytest

from nullion.chat_operator import _recent_unfinished_execution_context, _chat_ambiguity_classifier
from nullion.conversation_runtime import ConversationTurnDisposition as D


@pytest.mark.parametrize('state',['legacy_rejected','unfinished','complete','plain','reset','foreign','recovered'])
def test_only_typed_current_unfinished_execution_enables_relationship_recovery(state):
    event={'conversation_id':'web:a','turn_id':'failed','user_message':'Prepare the selected notice.',
        'assistant_reply':'Preparation stopped.','tool_results':[]}
    if state=='legacy_rejected':event['tool_results']=[{'status':'failed','output':{
        'reason':'request_plan_conflict','executed':False}}]
    if state in {'unfinished','complete'}:event['response_fulfilled']=state=='complete'
    if state=='foreign':event.update(conversation_id='web:b',response_fulfilled=False)
    if state=='recovered':event['tool_results']=[{'tool_name':'first','status':'failed','output':{
        'reason':'request_plan_conflict','executed':False}}, {'tool_name':'alternative','status':'completed','output':{}}]
    runtime=SimpleNamespace(store=SimpleNamespace(list_recent_conversation_events_after_reset=lambda *a,**kw:
        [] if state=='reset' else [event]))
    actual=_recent_unfinished_execution_context(runtime,conversation_id='web:a')
    assert bool(actual)==(state in {'legacy_rejected','unfinished'})


@pytest.mark.parametrize('prompt',['Continue','Continúa','続けて','تابع',
    'Continúa con el borrador, sin enviarlo. Incluye el número de pedido y todas las salsas que faltaron.',
    '先ほどの注文書に記載された注文番号と不足したソースの数を使い、管理者向けの下書きを続けてください。まだ送信しないでください。内容を確認できるよう、最終的な下書きの全文をここに表示してください。'])
def test_model_routes_compact_recovery_using_typed_state_and_prior_question(prompt):
    calls=[]
    class Provider:
        def create(self,**kw):
            calls.append(kw);body=json.dumps(kw['messages'],ensure_ascii=False)
            assert 'Receipt 05B33' in body and 'unfinished' in body
            return {'content':[{'type':'text','text':json.dumps({'relationship':'follow_up','effect':'continue',
                'confidence':1,'target_index':0})}]}
    fn,_=_chat_ambiguity_classifier([{'user':'Prepare the notice.','assistant':'Stopped.'}],model_client=Provider(),
        structured_followup_evidence=True,allow_plain_compact_classification=True,
        unfinished_execution_context={'turn_id':'failed','state':'unfinished'},
        previous_assistant_message='Receipt 05B33 was supplied. The selected action stopped.')
    assert fn(prompt,SimpleNamespace(active_branch_exists=True))==D.CONTINUE
    assert len(calls)==1


def test_healthy_plain_turn_does_not_add_a_relationship_model_call():
    class Provider:
        def create(self,**kw):pytest.fail('A healthy plain reply added a classifier call')
    fn,_=_chat_ambiguity_classifier([{'user':'Hello','assistant':'Hi'}],model_client=Provider(),
        previous_assistant_message='Hi')
    assert fn('Hola',SimpleNamespace(active_branch_exists=True)) is None


def test_attachment_relationship_receives_the_actual_previous_question_and_file_metadata():
    class Provider:
        def create(self, **kw):
            body = json.dumps(kw['messages'], ensure_ascii=False)
            assert 'Do you have the receipt?' in body
            assert 'receipt.png' in body and 'image/png' in body
            return {'content': [{'type': 'text', 'text': json.dumps({
                'relationship': 'follow_up', 'effect': 'continue',
                'confidence': 1, 'target_index': 0})}]}
    fn, _ = _chat_ambiguity_classifier(
        [{'user': 'Notify the restaurant.', 'assistant': 'Do you have the receipt?'}],
        model_client=Provider(), structured_followup_evidence=True,
        allow_plain_compact_classification=True,
        previous_assistant_message='Do you have the receipt?',
        attachment_context=({'name': 'receipt.png', 'content_type': 'image/png'},))
    assert fn('Please analyze the attached file(s).',
              SimpleNamespace(active_branch_exists=True)) == D.CONTINUE

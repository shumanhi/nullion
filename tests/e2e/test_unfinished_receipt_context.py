"""Real ingress, ancestry, preflight and transport after an unfinished request."""
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('legacy_broken_root',[False,True,'completed'])
@pytest.mark.parametrize('followup',['Continue','Continúa','続けて',
    'Continúa con el borrador, sin enviarlo. Incluye el número de pedido y todas las salsas que faltaron.'])
def test_failed_notification_keeps_receipt_and_resolution_without_reasking(tmp_path,monkeypatch,legacy_broken_root,followup):
    from fastapi.testclient import TestClient
    from nullion import web_app,telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _remember_chat_turn
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition as D
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    runtime=isolate(tmp_path,monkeypatch);registry=ToolRegistry();prepared=[];reviews=[];relations=[]
    def prepare(inv):
        prepared.append(inv.arguments)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'draft':'Order 05B33: all eleven sauces were missing.',
            'submitted':False})
    registry.register(ToolSpec('browser_run_task','Prepare a draft only; never sends a notification',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,2,capability_tags=('web',)),prepare)
    class Model:
        recovering=False
        def create(self,**kw):
            system=kw.get('system') or '';names={r['name'] for r in kw.get('tools',())}
            if 'relationship' in system and 'target_index' in system:
                relations.append(kw)
                if not self.recovering:time.sleep(1.7)
                body=json.dumps(kw['messages'],ensure_ascii=False)
                if legacy_broken_root != 'completed':
                    assert 'unfinished' in body
                return text(json.dumps({'relationship':'follow_up','effect':'continue','confidence':1,
                    'target_index':0,'target_is_explicit':False}))
            if 'Workflow action preflight.' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text']);reviews.append(packet)
                context=json.dumps(packet['request_context'],ensure_ascii=False)
                assert 'Receipt 05B33' in context and 'eleven sauces' in context
                assert 'management only' in context and '98th' in context
                return text(json.dumps({'decision':'proceed','issues':[],'issue_kind':'none','step_effect':'mutation'}))
            protocol=workflow_provider_reply(kw)
            if protocol is not None:return protocol
            if not self.recovering:return text('Hello.')
            if 'browser_run_task' not in names:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']})
            if not prepared:return call('browser_run_task',{'order_id':'05B33','missing_sauces':11,'resolution':'management only'})
            return text('Draft for management: Order 05B33 at 98th and Columbus was missing all eleven sauces. No refund is requested. Nothing has been submitted.\n\n1. Approve this draft\n2. Edit it')
    model=Model();runner=AgentOrchestrator(model_client=model);runtime.active_tool_registry=registry
    elapsed=[]
    with TestClient(web_app.create_app(runtime,orchestrator=runner,registry=registry)) as api:
        for prompt in ['Hola','こんにちは','مرحبا']:
            start=time.monotonic();response=api.post('/api/chat',json={'text':prompt,'conversation_id':'web:healthy'})
            elapsed.append(time.monotonic()-start);assert response.json()['text']=='Hello.'
    assert not relations and sum(elapsed)/len(elapsed)<1.0 and max(elapsed)<1.5,elapsed
    cid='telegram:123'
    receipt=runtime.process_conversation_message(conversation_id=cid,chat_id='123',
        user_message='Here is the receipt for the complaint at 98th and Columbus.',turn_id='receipt',request_id='receipt',
        dispatch_disposition=D.INDEPENDENT,checkpoint=False).turn
    _remember_chat_turn(runtime,chat_id='123',conversation_turn_id=receipt.turn_id,
        user_message=receipt.user_message,assistant_reply='Receipt item details. '*40+
            'Receipt 05B33: eleven sauces were requested and all were missing.',response_fulfilled=True)
    resolution=runtime.process_conversation_message(conversation_id=cid,chat_id='123',
        user_message='Notify management only. Prepare it for approval; do not request a refund. https://example.test/feedback',
        turn_id='resolution',request_id='resolution',dispatch_disposition=D.INTERRUPT,
        dispatch_dependency_turn_ids=('receipt',),checkpoint=False).turn
    _remember_chat_turn(runtime,chat_id='123',conversation_turn_id=resolution.turn_id,
        user_message=resolution.user_message,
        assistant_reply=('The draft for management is ready; nothing has been submitted.'
            if legacy_broken_root == 'completed' else 'The selected action did not execute.'),
        response_fulfilled=legacy_broken_root == 'completed',
        tool_results=([ToolResult('prepared', 'browser_run_task', 'completed', {
            'draft': 'Order 05B33: eleven sauces were missing.', 'submitted': False})]
            if legacy_broken_root == 'completed' else [ToolResult('rejected','prepare_notice','failed',{
                'reason':'request_plan_conflict','executed':False,'review_decision':'revise'})]))
    parent=resolution.turn_id
    if legacy_broken_root is True:
        broken=runtime.process_conversation_message(conversation_id=cid,chat_id='123',user_message='Continue',
            turn_id='broken',request_id='broken',dispatch_disposition=D.INDEPENDENT,checkpoint=False).turn
        _remember_chat_turn(runtime,chat_id='123',conversation_turn_id=broken.turn_id,user_message='Continue',
            assistant_reply='The action stopped.',response_fulfilled=False)
        parent=broken.turn_id
    model.recovering=True
    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
        operator_chat_id='123',model_client=model,agent_orchestrator=runner)
    sent=[]
    class Message:
        message_id=880;reply_to_message=None;chat=SimpleNamespace(id=123)
        async def reply_text(self,value,**kw):
            sent.append(value);return SimpleNamespace(message_id=881+len(sent))
    Message.text=followup;Message.chat.send_action=lambda **kw:asyncio.sleep(0)
    asyncio.run(service.on_message(SimpleNamespace(update_id=880,message=Message()),SimpleNamespace(bot=None)))
    assert len(prepared)==1 and reviews and relations,sent
    assert any('all eleven sauces' in value and '1. Approve' in value for value in sent),sent
    turns=[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn']
    assert turns[-1]['parent_turn_id']==parent
    assert 'How would you like to locate' not in turns[-1]['assistant_reply']

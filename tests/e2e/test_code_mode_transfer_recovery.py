"""Real ingress, SQLite, lineage and Monty; only provider/source IO is faked."""
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('overflow',[False,True])
def test_large_results_and_numeric_choice_keep_findings_without_research_replay(
        tmp_path,monkeypatch,surface,overflow,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply, is_tool_scope_request
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0')
    if overflow:monkeypatch.setenv('NULLION_CODE_MODE_MAX_TRANSFER_BYTES','64000')
    reads=[];originals=[];registry=ToolRegistry();expected=[2];mission=['']
    def external(inv):
        assert mission[0] in inv.flow_context['user_request'],inv.flow_context
        assert '1' in inv.flow_context['user_request']
        assert 'Continue with selected sources' in inv.flow_context['user_request']
        assert inv.flow_context['request_time_context']['accepted_local_date']
        reads.append(inv.invocation_id)
        time.sleep(8 if len(reads)>expected[0] else .1)
        hotel=inv.arguments['kind']=='hotel'
        if hotel and overflow:
            output={'records':[{'name':'Casa 37','price':86,'date':'2031-04-08','url':'https://source.test/hotel'}],
                'body':'x'*100000,'source_record_collections':[{'path':['records'],
                    'identity_fields':['url'],'context_fields':['name','price','date']}]}
        else:
            finding='Casa 37, $86, 2031-04-08' if hotel else 'Flight 37, 18:00, 2031-04-08'
            output={'result_kind':'source_observations','report_status':'completed','goal_status':'answered',
                'page_url':'https://source.test/'+inv.arguments['kind'],
                'observations':[{'label':inv.arguments['kind'],'value':finding,'quote':finding,
                    'source_url':'https://source.test/'+inv.arguments['kind']}],
                'captured_documents':[{'source_url':'https://source.test','main_document_text':'x'*2_500_000}]}
        originals.append(output)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',output)
    registry.register(ToolSpec('fixture_source_read','Read selected sources',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,5,capability_tags=('web',),input_schema={
            'type':'object','additionalProperties':False,'required':['kind'],
            'properties':{'kind':{'type':'string','enum':['flight','hotel']}}}),external)
    class Model:
        phase=0;errors=[]
        def create(self,**kw):
            if is_tool_scope_request(kw):
                return text(json.dumps({'web_action':'live_research','evidence_policy':'fresh',
                    'requested_tool_names':['fixture_source_read'],'required_tool_names':['fixture_source_read'],'confidence':1}))
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            try:return self.respond(kw)
            except Exception as e:self.errors.append(repr(e));raise
        def respond(self,kw):
            self.phase+=1
            if self.phase==1:return text('1. Continue with selected sources\n2. Choose another source')
            if self.phase==2:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['fixture_source_read'],'required_tool_names':['fixture_source_read']})
            if self.phase==3:return call('code_mode',{'tool_names':['fixture_source_read'],'code':
                "f=call_tool('fixture_source_read', {'kind':'flight'})\nh=call_tool('fixture_source_read', {'kind':'hotel'})\n"
                "{'flight':f['output']['observations'][0]['value'],'hotel':h['output']['observations'][0]['value']}"})
            assert self.phase==4,'No source restart or second program after completed results'
            packets=[json.loads(b['content'][0]['text']) for m in kw['messages']
                if isinstance(m.get('content'),list) for b in m['content']
                if isinstance(b,dict) and b.get('type')=='tool_result' and b.get('tool_use_id')=='code_mode-id']
            output=packets[-1]['output']
            if overflow:
                assert output['reason']=='code_mode_input_limit'
                assert output['transfer_overflow']['native_receipt_retained'] is True
                assert len(output['native_observations'])==2
                assert all(v in json.dumps(output['native_observations']) for v in ('18:00','Casa 37','86','2031-04-08'))
            else:
                assert output['reason']=='code_mode_completed'
                assert 'Casa 37' in str(output['value'])
                assert output['transfer_usage']['transferred_bytes']<128000
            return text('Flight 37 departs 2031-04-08 at 18:00. Casa 37 costs $86. https://source.test/hotel')
    model=Model()
    if surface=='telegram':
        from nullion.config import load_settings
        from nullion.telegram_app import ChatOperatorService
        settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
            'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
        service=ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
            operator_chat_id='123',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
    samples=[];message_number=[12000]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['2031-04-08、18:00、宿泊も確認。','Comprueba el vuelo a las 18:00 del 2031-04-08 y el hotel.',
                'تحقق من الرحلة في 2031-04-08 الساعة 18:00 والفندق.']):
            cid='web:large-choice-'+str(index) if surface=='web' else 'telegram:123'
            def send(value):
                if surface=='web':
                    response=client.post('/api/chat',json={'text':value,'conversation_id':cid})
                    assert response.status_code==200,response.text
                    return response.json()['text']
                message_number[0]+=1;sent=[]
                class Message:
                    text=value;message_id=message_number[0];reply_to_message=None
                    chat=SimpleNamespace(id=123,send_action=lambda **kw:asyncio.sleep(0))
                    async def reply_text(self,value,**kw):
                        sent.append(value);return SimpleNamespace(message_id=message_number[0]+100)
                asyncio.run(service.on_message(SimpleNamespace(update_id=message_number[0],message=Message()),SimpleNamespace(bot=None)))
                assert sent
                return sent[-1]
            prompt += ' https://source.test/stay'
            model.phase=0;mission[0]=prompt;expected[0]=len(reads)+2
            assert '1.' in send(prompt)
            started=time.monotonic();visible=send('1');samples.append(time.monotonic()-started)
            assert not model.errors,model.errors
            assert 'Casa 37' in visible and '18:00' in visible and '2031-04-08' in visible
            assert model.phase==4 and len(reads)==expected[0]
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            assert turn.get('parent_turn_id') and turn['response_fulfilled'] is True
            native=[r for r in turn['tool_results'] if r['tool_name']=='fixture_source_read']
            assert len(native)==2 and all(r['status']=='completed' for r in native)
            assert len([r for r in turn['tool_results'] if r['tool_name']=='code_mode'])==1
            assert originals[-2]['captured_documents'][0]['main_document_text']=='x'*2_500_000
    assert sum(samples)/len(samples)<5 and max(samples)<7,samples
    record_property('large_result_choice_mean_seconds',sum(samples)/len(samples))

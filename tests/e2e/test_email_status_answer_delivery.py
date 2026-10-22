"""Real chat/API, SQLite and Telegram delivery with external account/model fakes."""
import asyncio,json,os,time
from types import SimpleNamespace
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='container E2E only')

@pytest.fixture(autouse=True)
def _isolate_mailbox_runtime_context(monkeypatch):
 # Each case installs a separate fake mailbox in a different runtime home.
 # Recovery can warm reusable provider facts. Preserve the surrounding suite's
 # cache state so those fixture-only capabilities cannot leak into later chat.
 from nullion import connector_prompt_context as context
 for name in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE',
              '_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE'):
  monkeypatch.setattr(context,name,None)
 for name in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT',
              '_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT'):
  monkeypatch.setattr(context,name,0.0)

CASES=[
 ('Check my email: is a payment scheduled now?',"I can't confirm a payment scheduled now. The confirmation is for $3,000 on June 14, 2026, which is past.",'en'),
 ('Revisa mi correo: ¿hay un pago programado ahora?','No puedo confirmar un pago pendiente ahora. El correo confirma $3,000 para el 14 de junio de 2026, una fecha pasada.','es'),
 ('メールで現在の支払い予約を確認してください。','現在の支払い予約は確認できません。過去の確認メールは2026年6月14日の3,000ドルの支払いです。','ja'),
]
@pytest.mark.parametrize('draft_mode',['answer','preview','body_dump'])
@pytest.mark.parametrize('history_found',[True,False])
@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('prompt,answer,language',CASES)
def test_account_status_answer_survives_mixed_searches_and_reaches_platform(tmp_path,monkeypatch,surface,prompt,answer,language,history_found,draft_mode,recovery_mode="answer",current_confirmation=False,mixed_source=False):
 from nullion.runtime import bootstrap_persistent_runtime
 from nullion.tools import ToolRegistry,register_email_plugin
 from nullion.agent_orchestrator import AgentOrchestrator
 from nullion.config import NullionSettings,TelegramSettings
 from nullion.chat_operator import handle_chat_operator_message
 from nullion.telegram_app import _deliver_reply
 from nullion import web_app
 from fastapi.testclient import TestClient
 monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
 monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
 monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
 from nullion import connections
 from nullion.connections import ProviderConnection,ConnectionRegistry,save_connection_registry
 monkeypatch.setattr(connections,'_CONNECTIONS_PATH',tmp_path/'home'/'connections.json')
 save_connection_registry(ConnectionRegistry(connections=[ProviderConnection(
  connection_id='fixture',workspace_id='workspace_admin',provider_id='fixture_connector_provider',
  display_name='Fixture mailbox',provider_profile='https://mail.example.test',permission_mode='read')]))
 message={'id':'mail-old','subject':'Scheduling confirmation','from':'Service <service@example.test>',
  'date':'Thu, 11 Jun 2026 15:34:57 -0700','snippet':'Your request was scheduled.',
  'body':'Amount: $3,000.00. Scheduled date: June 14, 2026. This confirmation records the requested payment on the stated processing date. Review the account for later changes, completion, or cancellation. This message alone does not verify the current balance.'}
 if current_confirmation:
  message['date']='Mon, 12 Oct 2026 15:34:57 -0700'
  message['body']=message['body'].replace('June 14, 2026','October 14, 2026')
 if mixed_source:
  message.update(subject='Your order is on the way',snippet='Order FIXTURE447 is headed out.',
   body='Jacket shipped on October 13. Tracking number FIXTURE123. Track it at https://carrier.example.test/track/FIXTURE123.')
 searches=[];reads=[]
 def search(query,limit):
  searches.append(query)
  return [{k:v for k,v in message.items() if k!='body'}] if query=='history-window' and history_found else []
 def read(message_id):
  reads.append(message_id);return dict(message)
 registry=ToolRegistry();register_email_plugin(registry,email_searcher=search,email_reader=read)
 if mixed_source:
  # Fake only the external browser transport; keep scope, real browser tools,
  # runner, SQLite, final formatting, API and Telegram delivery intact.
  from nullion.plugins.browser_plugin import register_browser_tools
  from nullion.plugins.browser_plugin.backends.auto_backend import AutoBackend
  async def navigate(self,session_id,url):
   assert url=='https://carrier.example.test/track/FIXTURE123'
   return 'Navigated (status 200)'
  async def extract_text(self,session_id,selector=None):return 'Track a package. Sign in. No verified tracking event available.'
  async def close(self,*args,**kwargs):pass
  monkeypatch.setattr(AutoBackend,'navigate',navigate)
  monkeypatch.setattr(AutoBackend,'extract_text',extract_text)
  monkeypatch.setattr(AutoBackend,'close_session',close)
  monkeypatch.setenv('NULLION_BROWSER_BACKEND','auto')
  register_browser_tools(registry)
 class Model:
  calls=0
  def create(self,**kwargs):
   names={t['name'] for t in kwargs.get('tools',[])}
   if not names:
    # External intent/scope selection, never a core phrase detector.
    return {'content':[{'type':'text','text':json.dumps({'web_action':'none','scheduler_action':'none','requested_tool_names':['email_search','email_read'],'required_tool_names':['email_search'],'connector_source_user_requested':True,'connector_source_evidence':prompt,'confidence':1})}]}
   self.calls+=1
   def tool(name,args,id):return {'type':'tool_use','id':id,'name':name,'input':args}
   if self.calls==1:
    content=[tool('request_tool_scope',{'capabilities':['connector'],'source_user_requested':True,'source_evidence':prompt,'tool_names':['email_search','email_read'],'required_tool_names':['email_search']},'scope')]
   elif self.calls==2:
    assert all('event dates' in t['description'] for t in kwargs['tools'] if t['name'] in {'email_search','email_read'})
    content=[tool('email_search',{'query':'history-window','limit':5},'a'),tool('email_search',{'query':'current-window','limit':5},'b')]
   elif self.calls==3 and history_found:content=[tool('email_read',{'id':'mail-old'},'c')]
   elif mixed_source and self.calls in (4,5,6):
    if self.calls==4:content=[tool('request_tool_scope',{'capabilities':['web'],
      'tool_names':['browser_navigate','browser_extract_text'],'required_tool_names':['browser_navigate']},'web-scope')]
    elif self.calls==5:content=[tool('browser_navigate',{'url':'https://carrier.example.test/track/FIXTURE123'},'navigate')]
    else:content=[tool('browser_extract_text',{},'extract')]
   elif mixed_source and self.calls in (7,8):
    context=json.dumps(kwargs['messages'],ensure_ascii=False)
    assert 'Jacket shipped on October 13' in context and 'No verified tracking event available' in context
    if draft_mode=='preview' and self.calls==7:
     from nullion.response_sanitizer import _email_read_summary
     return {'stop_reason':'end_turn','content':[{'type':'text','text':_email_read_summary({'message':message})}]}
    return {'stop_reason':'end_turn','content':[{'type':'text','text':answer}]}
   elif self.calls==(4 if history_found else 3):
    context=json.dumps(kwargs['messages'],ensure_ascii=False)
    if history_found:assert 'current_account_state_verified' in context and ('2026-10-12T22:34:57+00:00' if current_confirmation else '2026-06-11T22:34:57+00:00') in context and message['body'] in context, context[-9000:]
    if history_found and draft_mode!='answer':
     from nullion.response_sanitizer import _email_read_summary
     draft=_email_read_summary({'message':message}) if draft_mode=='preview' else message['body']*5
     return {'stop_reason':'end_turn','content':[{'type':'text','text':draft}]}
    return {'stop_reason':'end_turn','content':[{'type':'text','text':answer}]}
   elif history_found and draft_mode!='answer' and (self.calls==5 or recovery_mode=='timeout' and self.calls==6):
    context=json.dumps(kwargs['messages'],ensure_ascii=False)
    assert 'Runtime UTC date:' in context and message['body'] in context
    assert 'original request' in context and 'event or processing dates' in context
    if recovery_mode=='timeout':raise TimeoutError('external model recovery timeout')
    if recovery_mode=='repeat':
     from nullion.response_sanitizer import _email_read_summary
     return {'stop_reason':'end_turn','content':[{'type':'text','text':_email_read_summary({'message':message})}]}
    return {'stop_reason':'end_turn','content':[{'type':'text','text':answer}]}
   else:
    time.sleep(3);raise AssertionError('An extra verification/classifier call entered the finished account answer')
   return {'stop_reason':'tool_use','content':content}
 if not history_found:
  answer={'en':'I cannot confirm a current payment. No confirmation was found in the checked 90-day window.',
   'es':'No puedo confirmar un pago pendiente. No hay confirmación en los últimos 90 días.',
   'ja':'現在の支払い予約は確認できません。直近90日間に確認メールは見つかりませんでした。'}[language]
 model=Model();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.active_tool_registry=registry
 from datetime import UTC,datetime
 from nullion.approvals import create_permission_grant
 from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL,BoundaryPolicyRule,BoundaryKind,PolicyMode
 for name in (('email_search','email_read','browser_navigate','browser_extract_text') if mixed_source else ('email_search','email_read')):
  runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-consent',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
   permission='tool:'+name,granted_by='fixture',granted_at=datetime.now(UTC)))
 runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-mail',OPERATOR_PERMISSION_PRINCIPAL,
  BoundaryKind.ACCOUNT_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
 orchestrator=AgentOrchestrator(model_client=model);conv='web:email-status' if surface=='web' else 'telegram:90200'
 if surface=='web':
  app=web_app.create_app(runtime,orchestrator=orchestrator,registry=registry)
  with TestClient(app) as client:
   r=client.post('/api/chat',json={'text':prompt,'conversation_id':conv});assert r.status_code==200,r.text;reply=r.json()['text']
 else:
  settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz',operator_chat_id='90200',chat_enabled=True))
  reply=handle_chat_operator_message(runtime,prompt,chat_id='90200',settings=settings,model_client=model,
    agent_orchestrator=orchestrator,append_activity_trace=False,allow_mini_agents=False)
 if history_found and draft_mode!='answer' and recovery_mode!='answer':
  answer=("time limit" if recovery_mode=='timeout' else "I read the email, but couldn't verify the answer to your question.")
 assert answer in reply
 assert 'Preview:' not in reply and 'From:' not in reply
 events=[e for e in runtime.store.list_conversation_events(conv) if e.get('event_type')=='conversation.chat_turn']
 assert answer in events[-1]['assistant_reply']
 results=events[-1]['tool_results']
 if mixed_source:
  browser=[r for r in results if r['tool_name'] in {'browser_navigate','browser_extract_text'}]
  assert len(browser)==2 and all(r['status']=='completed' for r in browser),browser
  assert 'No verified tracking event available' in browser[-1]['output']['text']
 bodies=[r for r in results if r['tool_name']=='email_read']
 if history_found:assert bodies[0]['output']['evidence_scope']['current_account_state_verified'] is False
 else:assert bodies==[]
 assert searches==['history-window','current-window'] and reads==(['mail-old'] if history_found else []) and model.calls==((8 if draft_mode=='preview' else 7) if mixed_source else (6 if recovery_mode=='timeout' else 5 if draft_mode!='answer' else 4) if history_found else 3)
 wire=[]
 class TelegramMessage:
  chat=SimpleNamespace(id=90200);message_id=1
  async def reply_text(self,text,**kwargs):wire.append((text,kwargs));return SimpleNamespace(message_id=2)
 asyncio.run(_deliver_reply(TelegramMessage(),reply,principal_id='telegram:90200'))
 assert len(wire)==1 and 'Preview:' not in wire[0][0]
 from html import unescape
 assert answer in unescape(wire[0][0])
 if history_found and recovery_mode=='answer' and not mixed_source:assert '$3,000' in wire[0][0] or '3,000ドル' in wire[0][0]
 # Delivery uses the configured runtime home; verify the emitted receipt too.
 receipts=list((tmp_path/'home').rglob('delivery_receipts.jsonl'))+list((tmp_path/'nullion-data').rglob('delivery_receipts.jsonl'))
 assert receipts and json.loads(receipts[0].read_text().splitlines()[-1])['status']=='succeeded'


@pytest.mark.parametrize('recovery_mode',['repeat','timeout'])
def test_email_preview_recovery_is_bounded_and_never_delivers_preview_on_failure(tmp_path,monkeypatch,recovery_mode):
 test_account_status_answer_survives_mixed_searches_and_reaches_platform(
  tmp_path,monkeypatch,'web',*CASES[0],True,'preview',recovery_mode)


@pytest.mark.parametrize('draft_mode',['answer','preview'])
def test_current_confirmation_is_accepted_with_its_actual_processing_date(tmp_path,monkeypatch,draft_mode):
 answer="The email confirms $3,000 scheduled for October 14, 2026. That confirms scheduling, not completed payment."
 test_account_status_answer_survives_mixed_searches_and_reaches_platform(
  tmp_path,monkeypatch,'web',CASES[0][0],answer,'en',True,draft_mode,current_confirmation=True)


def test_explicit_historical_request_can_confirm_the_historical_schedule(tmp_path,monkeypatch):
 prompt="Was a payment scheduled for June 14, 2026? Check my email for that date."
 answer="Yes. The email confirms $3,000 scheduled for June 14, 2026. That verifies the historical schedule, not whether it processed."
 test_account_status_answer_survives_mixed_searches_and_reaches_platform(
  tmp_path,monkeypatch,'web',prompt,answer,'en',True,'preview')


def test_plain_chat_latency_has_no_account_verification_call(tmp_path,monkeypatch,record_property):
 from fastapi.testclient import TestClient
 from nullion import web_app
 from nullion.runtime import bootstrap_persistent_runtime
 from nullion.tools import ToolRegistry,register_email_plugin
 from nullion.agent_orchestrator import AgentOrchestrator
 monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
 monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
 monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
 registry=ToolRegistry()
 register_email_plugin(registry,email_searcher=lambda *a:pytest.fail('Plain chat read email'),email_reader=lambda *a:pytest.fail('Plain chat read email'))
 class Model:
  calls=0
  def create(self,**kwargs):
   self.calls+=1
   if self.calls>1:
    time.sleep(3);raise AssertionError('Optional account classifier entered the ordinary hot path')
   return {'stop_reason':'end_turn','content':[{'type':'text','text':'56'}]}
 model=Model();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
 app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
 elapsed=[]
 with TestClient(app) as client:
  for index,prompt in enumerate(['7 × 8 の答えだけ。','Solo 7 × 8.','Calcule 7 × 8.']):
   model.calls=0;start=time.monotonic();r=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:plain-'+str(index)});elapsed.append(time.monotonic()-start)
   assert r.status_code==200 and r.json()['text']=='56',r.text
   assert model.calls==1
   records=[e for e in runtime.store.list_conversation_events('web:plain-'+str(index)) if e.get('event_type')=='conversation.chat_turn']
   assert records[-1]['tool_results']==[]
 record_property('elapsed_seconds',json.dumps(elapsed))
 assert sum(elapsed)/len(elapsed)<1.5 and max(elapsed)<2.5,elapsed


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('draft_mode',['answer','preview'])
@pytest.mark.parametrize('prompt,answer',[
 ('Where is my jacket? Can you check its tracking status?',
  'Your jacket shipped on October 13. Tracking number: FIXTURE123. I checked the carrier page but could not verify its current location or delivery estimate.'),
 ('¿Dónde está mi chaqueta? Comprueba el seguimiento.',
  'La chaqueta salió el 13 de octubre. Seguimiento: FIXTURE123. No pude confirmar la ubicación actual ni la fecha de entrega en la página del transportista.'),
 ('ジャケットはどこですか？配送状況を確認してください。',
  'ジャケットは10月13日に発送されています。追跡番号はFIXTURE123です。配送会社のページでは現在地と配達予定日を確認できませんでした。')])
def test_email_to_carrier_status_answer_is_not_replaced_by_source_preview(tmp_path,monkeypatch,surface,draft_mode,prompt,answer):
 test_account_status_answer_survives_mixed_searches_and_reaches_platform(
  tmp_path,monkeypatch,surface,prompt,answer,'en',True,draft_mode,mixed_source=True)

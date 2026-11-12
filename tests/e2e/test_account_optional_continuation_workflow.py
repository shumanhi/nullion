"""Successful mailbox outcomes cannot be replaced by optional link recovery."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker API boundary')

@pytest.fixture(autouse=True)
def isolate_provider_cache(monkeypatch):
 from nullion import connector_prompt_context as context
 for name in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE'):
  monkeypatch.setattr(context,name,None)
 for name in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT'):
  monkeypatch.setattr(context,name,0.0)

@pytest.mark.parametrize('mode',['optional','link','attachment'])
@pytest.mark.parametrize('prompt,answer',[
 ('Anything urgent in my email overnight?','No urgent messages: a receipt and a shipping update.'),
 ('夜間のメールに緊急の連絡は？','緊急の連絡はありません。領収書と発送通知が届いています。'),
 ('¿Hay correos urgentes de anoche?','No hay mensajes urgentes: un recibo y un aviso de envío.')])
def test_optional_mail_links_and_attachment_do_not_restart_finished_answer(tmp_path,monkeypatch,prompt,answer,mode):
 link_required=mode=='link'
 attachment_required=mode=='attachment'
 from fastapi.testclient import TestClient
 from nullion import web_app,chat_store
 from nullion.agent_orchestrator import AgentOrchestrator
 from nullion.runtime import bootstrap_persistent_runtime
 from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
 monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
 from nullion import connections
 monkeypatch.setattr(connections,'_CONNECTIONS_PATH',tmp_path/'connections.json')
 connections.save_connection_registry({'connections':[{'connection_id':'fixture-mail','workspace_id':'workspace_admin','provider_id':'fixture_connector_provider','display_name':'Fixture mailbox','active':True,'permission_mode':'read'}]})
 registry=ToolRegistry();calls=[]
 def account(inv):
  calls.append(inv.tool_name)
  if inv.tool_name=='email_search':out={'results':[{'id':'m37','subject':'Receipt','snippet':'Paid 37 USD.'}], 'body_requires_tool':'email_read','next_tool_for_body':'email_read'}
  elif inv.tool_name=='email_read':out={'message':{'id':'m37','subject':'Receipt','body':'Paid 37 USD. No action is needed.', 'attachments':[{'filename':'receipt.pdf'}],'next_tool':'email_attachment_read','attachment_requires_tool':'email_attachment_read','links':[{'url':'https://example.org/receipt','text':'Receipt'}],'next_tools_for_links':['browser_navigate','browser_extract_text','web_fetch']}}
  elif attachment_required and inv.tool_name=='email_attachment_read':out={'filename':'receipt.pdf','text':'Receipt verified for 37 USD.'}
  elif link_required and inv.tool_name in {'browser_navigate','browser_extract_text','web_fetch'}:out={'url':'https://example.org/receipt','text':'Receipt verified for 37 USD.'}
  else:raise AssertionError('An optional continuation ran: '+inv.tool_name)
  if inv.tool_name=='email_read' and link_required:out['message']['link_followup_required']=True
  return ToolResult(inv.invocation_id,inv.tool_name,'completed',out)
 for name in ['email_search','email_read','email_attachment_read','browser_navigate','browser_extract_text','web_fetch']:
  registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),account)
 class Provider:
  phase=0;errors=[];recovery_calls=0
  def create(self,**kw):
   protocol_reply = workflow_provider_reply(kw, completion=True)
   if protocol_reply is not None: return protocol_reply
   try:return self._create(**kw)
   except Exception as e:self.errors.append(repr(e));raise
  def _create(self,**kw):
   names={t['name'] for t in kw.get('tools',[])}
   def text(t):return {'stop_reason':'end_turn','content':[{'type':'text','text':t}]}
   def tool(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
   if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
   if not names:return text('{}')
   if 'request_tool_scope' not in names and names.intersection({'browser_navigate','email_attachment_read'}):
    assert link_required or attachment_required,'Optional links restarted the request'
    self.recovery_calls+=1
    assert 'Paid 37 USD.' in json.dumps(kw['messages'],ensure_ascii=False),'Recovery lost completed mailbox evidence'
    if attachment_required:
     if self.recovery_calls==1:return tool('email_attachment_read',{'message_id':'m37','attachment_id':'attachment-37'})
     assert self.recovery_calls==2
     return text(answer)
    if self.recovery_calls==1:return tool('browser_navigate',{'url':'https://example.org/receipt'})
    if self.recovery_calls==2:return tool('browser_extract_text',{})
    assert self.recovery_calls==3
    return text(answer)
   self.phase+=1
   if self.phase==1:return tool('request_tool_scope',{'capabilities':['connector'],'tool_names':['email_search','email_read'],'required_tool_names':['email_search','email_read',*(['email_attachment_read'] if attachment_required else [])],'source_user_requested':True,'connector_app_ids':['google-mail'],'source_evidence':prompt})
   if self.phase==2:return tool('email_search',{'query':'newer_than:1d'})
   if self.phase==3:return tool('email_read',{'id':'m37'})
   if attachment_required and self.phase==5:
    assert 'Paid 37 USD.' in json.dumps(kw['messages'],ensure_ascii=False)
    return tool('email_attachment_read',{'message_id':'m37','attachment_id':'attachment-37'})
   if attachment_required and self.phase==6:return text(answer)
   assert self.phase==4,'Completed account read was restarted'
   return text('The source was read; required link verification remains pending.' if link_required or attachment_required else answer)
 provider=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=provider
 from datetime import UTC,datetime
 from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,OPERATOR_PERMISSION_PRINCIPAL
 for kind in (BoundaryKind.ACCOUNT_ACCESS,BoundaryKind.OUTBOUND_NETWORK):
  runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-'+kind.value,OPERATOR_PERMISSION_PRINCIPAL,kind,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
 with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)) as client:
  r=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:account-finished'});assert r.status_code==200,r.text
  assert not provider.errors,provider.errors
  assert r.json()['text']==answer and not r.json()['artifacts'],r.json()
 assert not provider.errors,provider.errors
 assert calls==(['email_search','email_read','browser_navigate','browser_extract_text'] if link_required else ['email_search','email_read','email_attachment_read'] if attachment_required else ['email_search','email_read'])
 reopened=bootstrap_persistent_runtime(tmp_path/'runtime.db');events=reopened.store.list_conversation_events('web:account-finished');turns=[e for e in events if e.get('event_type')=='conversation.chat_turn']
 assert turns[-1]['assistant_reply'] in {answer,'✉️ '+answer}
 assert [r['tool_name'] for r in turns[-1]['tool_results']]==['request_tool_scope',*calls]

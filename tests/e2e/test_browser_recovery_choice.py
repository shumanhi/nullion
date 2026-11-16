"""Real chat recovery: reconnect, ask, then open headless only after choice."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('mode',['off','on'])
@pytest.mark.parametrize('prompt',['ページを確認して','Consulta la página','Vérifie la page'])
def test_model_only_headless_override_is_rejected_at_real_chat_boundary(tmp_path,monkeypatch,mode,prompt):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE',mode)
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','auto')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS','false')
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_open'],'required_tool_names':['browser_open']})
            if self.phase==2:
                return call('code_mode',{'tool_names':['browser_open'],
                    'code':"call_tool('browser_open', {'mode':'headless'})"}) if mode=='on' else call('browser_open',{'mode':'headless'})
            return text('The configured browser was preserved. No browser mode was changed.')
    registry=ToolRegistry();model=Model()
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:mode-guard'})
        assert response.status_code==200 and response.json()['text']
    rows=[r for e in runtime.store.list_conversation_events('web:mode-guard')
        if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
    opened=[r for r in rows if r['tool_name']=='browser_open']
    assert opened and all(r['status']=='failed' and r['output'].get('executed') is False for r in opened),rows
    assert all(r['output']['recovery']['argument_constraints']['mode']['enum']==['configured'] for r in opened)


@pytest.mark.parametrize('prompt', ['公開ページを調べて', 'Consulta la página pública'])
@pytest.mark.parametrize('entry',['browser_open','browser_read_pages'])
@pytest.mark.parametrize('scheduled',[False,True])
def test_recovery_waits_for_numeric_choice_and_keeps_other_tasks_configured(tmp_path, monkeypatch, prompt, source_summary_model_reply,entry,scheduled):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import auto_backend
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry, ToolInvocation
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'auto')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'false')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setattr(auto_backend, '_is_cdp_reachable', lambda: False)
    monkeypatch.setattr(auto_backend, '_reuse_preferred_cdp_if_available', lambda: False)
    monkeypatch.setattr(auto_backend, '_find_chrome', lambda: None)
    attempts = []
    original = auto_backend.AutoBackend.reconnect
    async def reconnect(self, session_id=None):
        attempts.append(session_id)
        return await original(self, session_id)
    monkeypatch.setattr(auto_backend.AutoBackend, 'reconnect', reconnect)
    visits = []
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            visits.append(self.headers.get('Cookie', ''))
            self.send_response(200); self.end_headers()
            self.wfile.write(b'<h1>Public result: 37</h1><p>This public listing is available without an account. The measured result is 37 and was read from this source page. No sign-in, booking, purchase or other account action is required to inspect this information.</p>')
        def log_message(self, *args): pass
    site = ThreadingHTTPServer(('127.0.0.1', 0), Site)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    url = f'http://localhost:{site.server_port}/'
    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            summary = source_summary_model_reply(kwargs)
            if summary is not None: return summary
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['web'], 'tool_names':[entry], 'required_tool_names':[entry]})
            if self.phase == 2: return call(entry, {'pages':[{'url':url,'read_kind':'text'}]} if entry=='browser_read_pages' else {})
            if self.phase == 3:
                return call('request_tool_scope', {'capabilities':['web'], 'tool_names':['browser_open','browser_navigate','browser_extract_text'], 'required_tool_names':['browser_open','browser_navigate','browser_extract_text']})
            if self.phase == 4: return call('browser_open', {'mode':'headless'})
            if self.phase == 5: return call('browser_navigate', {'url':url})
            if self.phase == 6: return call('browser_extract_text', {})
            assert self.phase == 7 and 'Public result: 37' in json.dumps(kwargs['messages'])
            return text('Public result: 37.')
    model = Model()
    registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda:model)
    try:
        with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
            if scheduled:
                from nullion.crons import CronJob
                from nullion.cron_delivery import record_cron_delivery_chat_turn
                from tests.e2e.test_scheduled_report_completion import run_scheduled,telegram_boundary
                from nullion.messaging_turn_graph import PlatformChatRequest,run_platform_chat_request
                from nullion.config import NullionSettings,TelegramSettings
                deliveries,send=telegram_boundary(monkeypatch)
                def record(job,conversation_id,delivery_channel,delivery_target,delivered_text,browser_recovery_choices=()):
                    record_cron_delivery_chat_turn(runtime.store,job,conversation_id=conversation_id,
                        delivery_channel=delivery_channel,delivery_target=delivery_target,delivered_text=delivered_text,
                        browser_recovery_choices=browser_recovery_choices)
                job=CronJob(id='choice37',name='確認',task=prompt+' '+url,schedule='* * * * *',
                    delivery_channel='telegram',delivery_target='123')
                result=run_scheduled(job,model,runtime,registry,send,record_chat_turn=record)
                assert result['response_fulfilled'] is False
                assert deliveries,{key:result.get(key) for key in ['text','reason','cron_delivery_status','cron_report_failed','cron_delivery_block_reason','cron_failure_notice']}
                first_text=deliveries[-1]['text']
            else:
                first = client.post('/api/chat', json={'text':prompt, 'conversation_id':'web:public'})
                assert first.status_code==200,first.text
                first_text=first.json()['text']
            assert '1.' in first_text and '2.' in first_text,first_text
            assert len(attempts) == 1 and not visits
            cid='telegram:123' if scheduled else 'web:public'
            if scheduled:
                parent=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
                settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='123',chat_enabled=True))
                response=run_platform_chat_request(runtime,PlatformChatRequest(platform='telegram',chat_id='123',
                    conversation_id=cid,text='2',settings=settings,model_client=model,
                    agent_orchestrator=AgentOrchestrator(model_client=model),append_activity_trace=False,
                    reply_context={'platform':'telegram','reply_to_text':parent['assistant_reply'],'reply_to_from_bot':True,'reply_to_message_id':'1'}))
                assert '37' in response.text,response
                assert send(job,'telegram','123',response.text)
            else:
                second = client.post('/api/chat', json={'text':'2', 'conversation_id':cid})
                assert second.status_code == 200 and '37' in second.json()['text'], second.text
            assert visits and all(not cookie for cookie in visits)
            # A separate task still uses the configured browser. The choice
            # must not mutate environment defaults or another task's backend.
            other = registry.invoke(ToolInvocation('other', 'browser_open', 'web_user', {}, capsule_id='other-task', flow_context={'browser_session_scope':'other-task'}))
            assert other.status == 'failed' and other.output['requires_user_choice'] is True
            assert os.environ['NULLION_BROWSER_BACKEND'] == 'auto'
            turns = [e for e in runtime.store.list_conversation_events(cid) if e.get('event_type') == 'conversation.chat_turn']
            assert any(r['tool_name']=='browser_open' and r['output'].get('browser_mode')=='headless' for r in turns[-1]['tool_results'])
    finally:
        site.shutdown(); site.server_close()

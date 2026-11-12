"""Saved style settings through real chat, scheduler, Doctor and platform delivery.

Only model responses and external Slack/Discord HTTP clients are scripted.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
import time
from types import SimpleNamespace

import pytest
from tests.e2e.test_tracker_status_delivery import setup, add, record
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


def isolate_preferences(tmp_path,monkeypatch):
    from nullion import preferences
    monkeypatch.setattr(preferences,'_PREFS_PATH',tmp_path/'preferences.json')
    monkeypatch.setattr(preferences,'_PROFILE_PATH',tmp_path/'profile.json')


@pytest.mark.parametrize('emoji,markdown',[('none','plain'),('expressive','light'),('minimal','full')])
def test_saved_policy_reaches_chat_scheduled_report_and_doctor_without_another_model_pass(tmp_path,monkeypatch,record_property,emoji,markdown):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.crons import CronJob,load_crons
    from nullion.cron_delivery import CronRunDeliveryCallbacks, run_cron_delivery_workflow
    from nullion.chat_store import get_chat_store
    from nullion.tools import ToolRegistry
    isolate_preferences(tmp_path,monkeypatch)
    systems=[]
    class Model:
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            systems.append(kw.get('system',''))
            return {'content':[{'type':'text','text':'🔔 **結果**\n\n- A: 43 → 45\n- B: 31 → 34\n\n[Source](https://example.test/result)'}]}
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    samples=[]
    with TestClient(app) as client:
        csrf=client.get('/api/session').json()['csrf_token'];headers={'x-nullion-csrf':csrf}
        result=client.post('/api/preferences',headers=headers,json={
            'emoji_level':emoji,'markdown_style':markdown,'response_structure':'bullets','tone':'formal'})
        assert result.status_code==200 and result.json()['ok']
        for index,prompt in enumerate(['結果は？','¿El resultado?','ما النتيجة؟']):
            started=time.monotonic()
            reply=client.post('/api/chat',headers=headers,json={'text':prompt,'conversation_id':f'web:policy-{index}'})
            assert reply.status_code==200,reply.text
            value=reply.json()['text'];assert '43 → 45' in value and '31 → 34' in value
            assert ('🔔' in value)==(emoji!='none')
            assert ('**' in value)==(markdown!='plain')
            samples.append(time.monotonic()-started)
        assert len(systems)==3, 'Formatting must not add an ordinary-turn model call'
        # Background callers with no chat preference history use that exact policy.
        def run(job,cid):
            turn=AgentOrchestrator(model_client=model).run_turn(conversation_id=cid,principal_id='operator',
                user_message=job.task,conversation_history=[],tool_registry=ToolRegistry(),
                policy_store=runtime.store,approval_store=runtime.store)
            return {'final_text':turn.final_text}
        delivered=[]
        outcome=run_cron_delivery_workflow(CronJob(id='policy-report',name='確認',task='結果を表示',
            schedule='0 0 1 1 *',delivery_channel='web',delivery_target='web:report'),label='Scheduled task',
            callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'web',delivery_target=lambda j,c:'web:report',
                run_agent_turn=run,record_event=lambda *a,**kw:None,block_reason=lambda *a:None,
                save_web_delivery=lambda j,c,text,a,r:delivered.append(text) or True,
                send_platform_delivery=lambda *a:False))
        assert not outcome.get('cron_delivery_failed'),outcome
        assert len(delivered)==1 and ('⏰' in delivered[0])==(emoji!='none')
        assert '43 → 45' in delivered[0] and '31 → 34' in delivered[0]
        j=add(client,name='確認');now=datetime.now(timezone.utc)
        for i in range(12):record(db,j,'incomplete',now-timedelta(seconds=120-i*5))
        assert app.state.tracker_status_tick(load_crons(refresh_next_runs=False),now)==1
        notice=get_chat_store().load_messages('web:fixture',limit=50)[-1]['text']
        assert ('⚠' in notice)==(emoji!='none') and 'last 12 checks' in notice
        assert 'Latest recorded problem' in notice and 'No repair has been confirmed' in notice
    assert len(systems)==4
    assert all(s.count('Shared presentation policy')==1 for s in systems)
    assert all('formal' in s for s in systems)
    assert sum(samples)/len(samples)<1.5 and max(samples)<2.5
    record_property('presentation_plain_chat_average_seconds',sum(samples)/len(samples))


@pytest.mark.parametrize('channel',['slack','discord'])
@pytest.mark.parametrize('emoji,markdown',[('none','plain'),('expressive','light'),('expressive','full')])
def test_shared_message_contract_reaches_other_platform_bytes_and_preserves_artifact(tmp_path,monkeypatch,channel,emoji,markdown):
    from nullion.preferences import save_preferences
    from nullion.chat_text import UserMessage,ReplySection
    from nullion.slack_app import send_slack_platform_delivery
    from nullion.discord_app import send_discord_platform_delivery
    isolate_preferences(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'data'))
    save_preferences({'emoji_level':emoji,'markdown_style':markdown})
    artifact=tmp_path/'outputs'/'presentation.csv';artifact.parent.mkdir();artifact.write_bytes(b'name,value\nCedar,37\n')
    body=UserMessage(title='結果',kind='update',sections=(ReplySection('変化',('43 → 45','31 → 34')),),
        choices=('継続','停止'),footer='MEDIA:'+str(artifact))
    calls=[]
    if channel=='slack':
        class Client:
            def __init__(self,**kw):pass
            async def chat_postMessage(self,**kw):calls.append(kw);return {'ok':True,'ts':'1'}
            async def files_upload_v2(self,**kw):
                calls.append({**kw,'bytes':__import__('pathlib').Path(kw['file']).read_bytes()});return {'ok':True}
        monkeypatch.setattr('slack_sdk.web.async_client.AsyncWebClient',Client)
        ok=asyncio.run(send_slack_platform_delivery(bot_token='synthetic',channel='C1',text=body))
    else:
        class Client:
            def __init__(self,**kw):pass
            async def __aenter__(self):return self
            async def __aexit__(self,*a):pass
            async def post(self,url,**kw):
                record=kw.get('json') or kw.get('data') or {}
                if kw.get('files'):record={**record,'bytes':kw['files']['file'][1].read()}
                calls.append(record);return SimpleNamespace(raise_for_status=lambda:None)
        monkeypatch.setattr('httpx.AsyncClient',Client)
        ok=asyncio.run(send_discord_platform_delivery(bot_token='synthetic',channel_id='42',text=body))
    assert ok and len(calls)==1,calls
    visible=calls[0].get('initial_comment') or calls[0].get('content') or calls[0].get('text')
    assert '43 → 45' in visible and '31 → 34' in visible
    assert '\n1. 継続\n2. 停止' in visible and visible.count('1. ')==1
    assert ('🔔' in visible)==(emoji!='none')
    if channel=='slack':assert '## ' not in visible
    assert calls[0]['bytes']==artifact.read_bytes()
    assert 'MEDIA:' not in visible and str(tmp_path) not in visible


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in real stage rendering acceptance')
def test_live_stage_common_contract_restores_sections_literals_and_numeric_choices():
    """No provider call: save a common-contract message and render real stage UI."""
    from pathlib import Path
    from uuid import uuid4
    from playwright.sync_api import sync_playwright,expect
    from nullion.chat_text import UserMessage,ReplySection,render_user_message
    from nullion.preferences import Preferences
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753')
    assert base=='http://127.0.0.1:8753'
    cid='web:live-ui-audit:presentation-'+uuid4().hex
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        try:
            context=browser.new_context(viewport={'width':430,'height':932})
            context.add_init_script("localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
            saved=context.request.get(base+'/api/preferences').json()
            prefs=Preferences(**{k:saved[k] for k in ('emoji_level','markdown_style','response_structure')})
            literal='print("**literal** 🦊")'
            message=render_user_message(UserMessage(title='Shared presentation acceptance',kind='update',
                summary='Two confirmed changes.',sections=(ReplySection('Results',('District A\n43 → 45','District B\n31 → 34')),),
                paragraphs=('```python\n'+literal+'\n```','[Source](https://example.test/result?x=43&y=45)'),
                choices=('Continue for 1 hour','Stop'),footer='Ends at the saved deadline.'),preferences=prefs)
            response=context.request.post(base+'/api/chat/history/'+cid+'/message',data={'role':'bot','text':message})
            assert response.ok,response.text()
            page=context.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
            page.goto(base,wait_until='domcontentloaded')
            bubble=page.locator('#messages .msg.bot .bubble').filter(has_text='Shared presentation acceptance')
            expect(bubble).to_have_count(1)
            expect(bubble).to_contain_text('43 → 45');expect(bubble).to_contain_text('31 → 34')
            expect(bubble.locator('pre')).to_contain_text(literal)
            expect(bubble.locator('ol li')).to_have_count(2)
            expect(bubble.locator('ol li').first).to_have_text('Continue for 1 hour')
            assert bubble.locator('ol li').first.get_attribute('value')=='1'
            assert bubble.locator('a').get_attribute('href')=='https://example.test/result?x=43&y=45'
            assert ('🔔' in bubble.inner_text())==(saved['emoji_level']!='none')
            page.reload(wait_until='domcontentloaded');expect(bubble).to_have_count(1)
            expect(bubble.locator('ol li')).to_have_count(2)
            expect(bubble.locator('ol li').first).to_have_text('Continue for 1 hour')
            assert bubble.locator('ol li').first.get_attribute('value')=='1'
            assert not errors,errors
            out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'common-contract'
            out.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(out/'stage-mobile.png'),full_page=True)
            (out/'result.json').write_text(json.dumps({'conversation_id':cid,'preferences':saved,
                'authored_text':message,'rendered_text':bubble.inner_text(),'rendered_html':bubble.inner_html(),
                'new_model_requests':0},ensure_ascii=False,indent=2))
        finally:browser.close()


@pytest.mark.parametrize('markdown',['plain','light','full'])
@pytest.mark.parametrize('emoji',['none','expressive'])
def test_common_contract_formats_in_real_web_for_every_markdown_preference(tmp_path,monkeypatch,markdown,emoji):
    from nullion.chat_text import UserMessage,ReplySection,render_user_message
    from nullion.preferences import Preferences
    from playwright.sync_api import sync_playwright,expect
    from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port
    isolate_preferences(tmp_path,monkeypatch)
    app,runtime,db=setup(tmp_path,monkeypatch)
    cid='web:presentation-markdown'
    literal='## code heading\n1. literal list\n**literal**'
    message=render_user_message(UserMessage(title='結果',summary='Overview',kind='update',
        sections=(ReplySection('比較',('A\n43 → 45','B\n31 → 34')),),
        choices=('Continue','Stop'),footer='```text\n'+literal+'\n```'),
        preferences=Preferences(emoji_level=emoji,markdown_style=markdown))
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        try:
            context=browser.new_context(viewport={'width':430,'height':932})
            context.add_init_script("localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
            assert context.request.post(server.base_url+'/api/chat/history/'+cid+'/message',
                data={'role':'bot','text':message}).ok
            page=context.new_page();page.goto(server.base_url,wait_until='domcontentloaded')
            bubble=page.locator('#messages .msg.bot .bubble').filter(has_text='Overview')
            expect(bubble).to_have_count(1);expect(bubble.locator('ul li')).to_have_count(2)
            expect(bubble.locator('ol li')).to_have_count(2)
            assert bubble.locator('ul li').first.inner_text()=='A\n43 → 45'
            expect(bubble.locator('ul li').first.locator('br')).to_have_count(1)
            expect(bubble.locator('pre')).to_contain_text(literal)
            if markdown=='full':expect(bubble.locator('h2.message-heading')).to_have_count(2)
            else:expect(bubble.locator('h2')).to_have_count(0)
            if markdown=='plain':expect(bubble.locator('strong')).to_have_count(0)
            elif markdown=='light':expect(bubble.locator('strong')).to_have_count(2)
            assert bubble.locator('ol li').first.get_attribute('value')=='1'
            page.reload(wait_until='domcontentloaded');expect(bubble.locator('ol li')).to_have_count(2)
        finally:browser.close()

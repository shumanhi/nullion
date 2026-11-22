"""Native adapter callbacks and SDK HTTP paths; only remote services are faked."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import inspect
import json
import os
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container SDK E2E')


class Wire:
    def __init__(self,platform):
        self.platform=platform
        self.messages={9:'UNOWNED'}
        self.next_id=100
        self.calls=[]
        self.mode=None
        self.uncertain_id=None
        self.errors=[]
        self.hold_next_send=False
        self.accepted=asyncio.Event()
        self.release_ack=asyncio.Event()
    async def hold_ack(self,operation,payload):
        if self.hold_next_send and operation=='send' and 'RESULT37' not in self.content(payload):
            self.hold_next_send=False
            self.accepted.set()
            await self.release_ack.wait()
    def content(self,payload):
        return payload.get('text',payload.get('content',''))
    def apply(self,operation,payload,identity=None):
        text=self.content(payload)
        self.calls.append((operation,identity,text))
        if operation!='delete':
            units=len(text.encode('utf-16-le'))//2
            limit=4096 if self.platform=='telegram' else 2000
            if units>limit:self.errors.append((operation,units));return 'rejected',None
        failure=self.mode if operation==('edit' if self.mode=='edit_unknown' else 'send') and 'RESULT37' not in text else None
        if failure:self.mode=None
        if failure=='rejected':return 'rejected',None
        if operation=='send':
            self.next_id+=1;identity=self.next_id
            self.messages[identity]=text
        elif operation=='edit':self.messages[int(identity)]=text
        else:self.messages.pop(int(identity),None)
        if failure in {'unknown','edit_unknown'}:
            if failure=='unknown':self.uncertain_id=identity
            return 'unknown',identity
        return 'ok',identity
    async def telegram(self,request):
        method=request.url.path.rsplit('/',1)[-1]
        if method=='getMe':return httpx.Response(200,json={'ok':True,'result':{'id':12345,'is_bot':True,'first_name':'Fixture'}})
        if method=='sendChatAction':return httpx.Response(200,json={'ok':True,'result':True})
        payload={k:v[0] for k,v in parse_qs((await request.aread()).decode()).items()}
        operation={'sendMessage':'send','editMessageText':'edit','deleteMessage':'delete'}[method]
        outcome,identity=self.apply(operation,payload,int(payload['message_id']) if 'message_id' in payload else None)
        await self.hold_ack(operation,payload)
        if outcome=='rejected':return httpx.Response(400,json={'ok':False,'error_code':400,'description':'Explicit API rejection'})
        if outcome=='unknown':raise httpx.ReadTimeout('Accepted; response lost',request=request)
        result=True if operation=='delete' else {'message_id':identity,'date':1,'chat':{'id':77,'type':'private'},'text':payload['text']}
        return httpx.Response(200,json={'ok':True,'result':result})
    async def discord(self,request):
        from aiohttp import web
        def response(data,*,status=200,headers=None):
            return web.Response(body=json.dumps(data).encode(),status=status,
                headers={'Content-Type':'application/json',**(headers or {})})
        user={'id':'12345','username':'Fixture','discriminator':'0','bot':True,'avatar':None}
        if request.path.endswith('/users/@me'):return response(user)
        if request.path.endswith('/oauth2/applications/@me'):
            return response({'id':'12345','name':'Fixture','description':'','icon':None,
                'bot_public':True,'bot_require_code_grant':False,'owner':user,'verify_key':'fixture','flags':0})
        if request.method=='GET' and request.path.endswith('/channels/77'):
            return response({'id':'77','type':1,'recipients':[user]})
        payload=await request.json() if request.method!='DELETE' else {}
        operation={'POST':'send','PATCH':'edit','DELETE':'delete'}[request.method]
        identity=int(request.path.rsplit('/',1)[-1]) if operation!='send' else None
        outcome,identity=self.apply(operation,payload,identity)
        await self.hold_ack(operation,payload)
        headers={'X-RateLimit-Limit':'100','X-RateLimit-Remaining':'99',
            'X-RateLimit-Reset-After':'0.01','X-RateLimit-Bucket':'fixture-messages'}
        if outcome=='rejected':return response({'code':50035,'message':'Explicit API rejection'},status=400)
        if outcome=='unknown':
            request.transport.close()
            return web.Response()
        if operation=='delete':return web.Response(status=204,headers=headers)
        return response({'id':str(identity),'channel_id':'77','author':user,'content':payload['content'],
            'timestamp':'2026-11-21T00:00:00+00:00','edited_timestamp':None,'tts':False,
            'mention_everyone':False,'mentions':[],'mention_roles':[],'attachments':[],'embeds':[],
            'pinned':False,'type':0,'flags':0,'components':[]},headers=headers)


@asynccontextmanager
async def adapter_owner(tmp_path,monkeypatch,platform,wire):
    from nullion import telegram_app
    from nullion.config import load_settings
    from nullion.platform_activity import PlatformTaskCardStore,platform_activity_capabilities
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN,install_wire
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','task')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','true')
    class Owner:
        def set_deliver_fn(self,function):self.deliver_fn=function
    owner=Owner()
    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77',
        'NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service=telegram_app.ChatOperatorService(runtime,TOKEN,'77',settings,agent_orchestrator=owner)
    if platform=='telegram':
        install_wire(monkeypatch,SimpleNamespace(handle=wire.telegram,errors=[]))
        class Application:
            def run_polling(self,**kwargs):pass
        service._task_card_store=PlatformTaskCardStore(replace(platform_activity_capabilities('telegram'),min_update_interval_seconds=0))
        service.run_polling(application=Application())
        yield owner.deliver_fn,service._task_card_store
    else:
        import discord
        from aiohttp import web
        from nullion import discord_app
        application=web.Application();application.router.add_route('*','/{tail:.*}',wire.discord)
        runner=web.AppRunner(application);await runner.setup()
        server=web.TCPSite(runner,'127.0.0.1',0);await server.start()
        port=runner.addresses[0][1]
        monkeypatch.setattr(discord.http.Route,'BASE',f'http://127.0.0.1:{port}/api/v10')
        ready,release=asyncio.Event(),asyncio.Event()
        async def start(client,token,**kwargs):
            await client.login(token)
            ready.set()
            await release.wait()
        monkeypatch.setattr(discord.Client,'start',start)  # The external Gateway is not needed for HTTP delivery.
        monkeypatch.setenv('NULLION_DISCORD_ENABLED','true')
        monkeypatch.setenv('NULLION_DISCORD_BOT_TOKEN','synthetic-fixture-token')
        env_path=tmp_path/'discord-fixture.env'
        env_path.write_text('NULLION_DISCORD_ENABLED=true\nNULLION_DISCORD_BOT_TOKEN=synthetic-fixture-token\nNULLION_DISCORD_OPERATOR_USER_ID=77\n')
        env_path.chmod(0o600)
        running=asyncio.create_task(discord_app.run_discord_app(checkpoint_path=runtime.checkpoint_path,
            env_path=env_path,service_builder=lambda **kwargs:service))
        try:
            async with asyncio.timeout(5):
                while not ready.is_set():
                    if running.done():await running
                    await asyncio.sleep(.005)
            store=inspect.getclosurevars(owner.deliver_fn).nonlocals['_task_card_store']
            store._capabilities=replace(store._capabilities,min_update_interval_seconds=0)
            yield owner.deliver_fn,store
        finally:
            release.set();await running;await runner.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize('platform',['telegram','discord'])
@pytest.mark.parametrize('failure',['none','rejected','unknown','edit_unknown'])
async def test_long_cards_keep_all_tasks_native_receipts_and_independent_final(tmp_path,monkeypatch,platform,failure):
    from nullion.delivery_acknowledgement import UnconfirmedDelivery
    from nullion.task_queue import TaskRecord,TaskPriority,TaskStatus
    from nullion.task_status_format import format_task_status_summary
    from nullion.tools import ToolResult
    from nullion.run_activity import format_tool_activity_line
    wire=Wire(platform)
    tasks=[TaskRecord(f't{i}','group',f'{platform}:77','telegram:77',f'{i:02d} '+('🧭'*45)+'比較',
        'Compara opciones',TaskStatus.RUNNING,TaskPriority.NORMAL,[],[]) for i in range(20)]
    def summary():return format_task_status_summary(tasks,planner_summary='Parallel Mission',subject='比較 '+('🧭'*175))
    async with adapter_owner(tmp_path,monkeypatch,platform,wire) as (deliver,store):
        conversation=f'{platform}:77'
        async def update(text=None,**kwargs):
            return await deliver(conversation,text or summary(),is_status=True,group_id='group',
                status_kind=kwargs.pop('status_kind','task_summary'),**kwargs)
        assert await update()
        assert len([row for row in wire.calls if row[0]=='send'])>=1
        # A native completed tool's private output never becomes activity text.
        for index in range(6):
            body='\n'.join(format_tool_activity_line(ToolResult(f'i{index}-{j}',f'opaque_{index}_{j}_'+('x'*53),
                'completed',{'summary':'PRIVATE /secret/credentials.json'})) for j in range(3))
            assert await update(body,status_kind='tool_activity',activity_id=f'activity-{index}',activity_label='🧭'*50)
        all_status='\n'.join(value for key,value in wire.messages.items() if key!=9)
        assert 'PLANNER' in all_status and 'For:' in all_status and '20 tasks' in all_status
        assert 'opaque_5_2_' in all_status and 'PRIVATE' not in all_status
        for task in tasks:assert task.title in all_status
        assert len(wire.messages)>2 and not wire.errors
        before_send=sum(row[0]=='send' for row in wire.calls)
        if failure=='edit_unknown':
            wire.mode='edit_unknown'
            try:await update(summary()+'\nCambio de estado')
            except Exception:pass  # Known-ID edits may retry without appending a replacement.
            assert await update(summary()+'\nCambio de estado')
            assert sum(row[0]=='send' for row in wire.calls)==before_send
        elif failure!='none':
            original_titles=[task.title for task in tasks]
            monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','false')
            for index,task in enumerate(tasks):task.title=f'{index:02d} 確認'
            assert await update()  # One shorter card, same twenty owned tasks.
            for task,title in zip(tasks,original_titles):task.title=title
            monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','true')
            wire.mode=failure
            # Restoring the same full task/activity state needs more parts;
            # an unknown append must not be replayed on the next status tick.
            expanded=summary()
            try:await update(expanded)
            except Exception:pass
            assert wire.mode is None,'The fixture never crossed a new message boundary'
            if failure=='unknown':
                before=len(wire.calls)
                with pytest.raises(UnconfirmedDelivery):await update(expanded)
                assert len(wire.calls)==before and wire.uncertain_id in wire.messages
            else:
                assert await update(expanded)
                assert all(task.title in '\n'.join(wire.messages.values()) for task in tasks)
        if failure!='unknown':
            monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','false')
            for index,task in enumerate(tasks):task.title=f'{index:02d} 確認';task.status=TaskStatus.COMPLETE
            assert await update(terminal=True)
            # All twenty rows survive the shorter card, and surplus owned
            # native messages are removed instead of remaining as stale pages.
            assert len(wire.messages)==2
            for task in tasks:assert task.title in '\n'.join(wire.messages.values())
        assert await deliver(conversation,'RESULT37: 完了。Archivo preparado.',is_final=True,group_id='group')
        assert any('RESULT37' in value for value in wire.messages.values())
        before=len(wire.calls)
        await update('stale callback after final')
        assert len(wire.calls)==before and wire.messages[9]=='UNOWNED'
        assert not wire.errors


@pytest.mark.asyncio
@pytest.mark.parametrize('platform',['telegram','discord'])
async def test_final_does_not_wait_for_status_ack_or_allow_later_appends(tmp_path,monkeypatch,platform):
    from nullion.task_queue import TaskRecord,TaskPriority,TaskStatus
    from nullion.task_status_format import format_task_status_summary
    wire=Wire(platform)
    tasks=[TaskRecord(f't{i}','group',f'{platform}:77','telegram:77',f'{i:02d} 確認',
        '比較',TaskStatus.RUNNING,TaskPriority.NORMAL,[],[]) for i in range(20)]
    async with adapter_owner(tmp_path,monkeypatch,platform,wire) as (deliver,store):
        async def update():
            return await deliver(f'{platform}:77',format_task_status_summary(tasks,planner_summary='Parallel Mission',
                subject='比較 '+('🧭'*175)),
                is_status=True,group_id='group',status_kind='task_summary')
        assert await update()
        assert len(wire.messages)==2
        if platform=='telegram':
            from nullion.run_activity import format_tool_activity_line
            from nullion.tools import ToolResult
            for index in range(6):
                body='\n'.join(format_tool_activity_line(ToolResult(f'i{index}-{j}',f'opaque_{index}_{j}_'+('x'*53),
                    'completed',{'summary':'PRIVATE /secret/credentials.json'})) for j in range(3))
                assert await deliver(f'{platform}:77',body,is_status=True,group_id='group',
                    status_kind='tool_activity',activity_id=f'activity-{index}',activity_label='🧭'*50)
        for index,task in enumerate(tasks):task.title=f'{index:02d} '+('🧭'*45)+'比較'
        wire.hold_next_send=True
        in_flight=asyncio.create_task(update())
        try:
            await asyncio.wait_for(wire.accepted.wait(),3)
            async with asyncio.timeout(3):
                assert await deliver(f'{platform}:77','RESULT37: 最終結果',is_final=True,group_id='group')
            assert not in_flight.done(),'The remote status acknowledgement must still be held'
            before=len(wire.calls)
            wire.release_ack.set()
            await in_flight
            # Retain the acknowledged task-owned page; do not append remaining
            # pages or resurrect a card after a terminal result closed its state.
            assert len(wire.calls)==before
            await update()
            assert len(wire.calls)==before
            assert any('RESULT37' in text for text in wire.messages.values())
            assert wire.messages[9]=='UNOWNED' and not wire.errors
        finally:
            wire.release_ack.set()
            if not in_flight.done():await in_flight


@pytest.mark.asyncio
@pytest.mark.parametrize('platform',['telegram','discord'])
async def test_final_cancels_queued_native_status_edit(tmp_path,monkeypatch,platform):
    wire=Wire(platform)
    async with adapter_owner(tmp_path,monkeypatch,platform,wire) as (deliver,store):
        assert await deliver(f'{platform}:77','PLANNER\nFor: 比較\n1 task\n  ☐ 確認',
            is_status=True,group_id='group',status_kind='task_summary')
        store._capabilities=replace(store._capabilities,min_update_interval_seconds=.2)
        assert await deliver(f'{platform}:77','✓ list_crons',is_status=True,group_id='group',
            status_kind='tool_activity',activity_id='activity',activity_label='確認')
        assert store._pending
        assert await deliver(f'{platform}:77','RESULT37: 完了',is_final=True,group_id='group')
        before=len(wire.calls)
        await asyncio.sleep(.25)
        assert len(wire.calls)==before and not store._pending
        assert wire.messages[9]=='UNOWNED' and not wire.errors

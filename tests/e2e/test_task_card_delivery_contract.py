"""Real aggregation, Web server and rendered planner/activity settings boundary."""
import asyncio
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import os
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container Web UI')


@contextmanager
def _async_owner():
    """Keep native aggregation tasks on one loop beside Playwright's loop."""
    loop = asyncio.new_event_loop()
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(loop.run_forever)
        try:
            yield loop
        finally:
            async def close_tasks():
                pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            asyncio.run_coroutine_threadsafe(close_tasks(), loop).result(timeout=5)
            loop.call_soon_threadsafe(loop.stop)
            running.result(timeout=5)
            loop.close()


@pytest.mark.parametrize('planner,verbose', [(False,False),(False,True),(True,False),(True,True)])
def test_native_aggregator_renders_current_work_and_preserves_quiet_final(tmp_path, monkeypatch, planner, verbose):
    from playwright.sync_api import expect
    from nullion.mini_agent_runner import ProgressUpdate
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRegistry, TaskGroup, TaskRecord, TaskPriority, TaskStatus
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.run_activity import format_tool_activity_line
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'task' if planner else 'off')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'true' if verbose else 'false')
    class DeliveryOwner:
        deliver_fn = None
        def set_deliver_fn(self, function): self.deliver_fn = function
    owner = DeliveryOwner()
    with _UvicornThread(create_app(runtime, owner, ToolRegistry()), _free_port()) as server, _async_owner() as owner_loop:
        pw, browser, page, errors = _open_page(server.base_url)
        try:
            expect(page.locator('#status-label')).to_contain_text('Connected')
            conversation = page.evaluate("localStorage.getItem('nullion_conv_id')")
            registry = TaskRegistry()
            task = TaskRecord('task','group',conversation,'telegram_chat','確認','確認',
                TaskStatus.RUNNING,TaskPriority.NORMAL,[],[])
            asyncio.run_coroutine_threadsafe(registry.add_group(TaskGroup('group',conversation,'依頼',[task],
                planner_metadata={'disposition':'sequential_mission','tasks':['確認']})), owner_loop).result(timeout=5)
            observed = []
            def deliver(conversation_id, text, **kwargs):
                observed.append(kwargs)
                return owner.deliver_fn(conversation_id, text, **kwargs)
            aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry,
                outbox_path=tmp_path/'outbox.json')
            def update(kind, message=None, data=None):
                asyncio.run_coroutine_threadsafe(aggregator._handle(
                    ProgressUpdate('agent','task','group',kind,message,data)), owner_loop).result(timeout=5)
            update('task_started')
            card = page.locator('.task-status-bubble[data-task-group-id="group"]')
            if planner:
                expect(card).to_contain_text('確認')
                expect(card.locator('.task-status-planner-label')).to_contain_text('Planner')
            else:
                expect(card).to_have_count(0)
            secret = '/private/provider/credentials.json secret-payload'
            result = ToolResult('tool','opaque_read','completed',{'summary':secret})
            update('tool_activity', format_tool_activity_line(result), {'tool_status':'completed'})
            activity = page.locator('#messages .run-activity').filter(has_text='opaque_read')
            expect(activity).to_have_count(1 if verbose else 0)
            expect(page.locator('#messages')).not_to_contain_text(secret)
            if not planner:
                expect(page.locator('#messages')).not_to_contain_text('PLANNER')
                expect(card).to_have_count(0)
                if verbose:
                    only_activity = page.locator('.task-activity-bubble[data-task-group-id="group"]')
                    expect(only_activity.locator('.run-activity')).to_have_count(1)
                    expect(only_activity.locator('.task-status-title')).to_have_count(0)
            assert registry.get_task('task').status is TaskStatus.RUNNING
            assert observed[-1]['tool_status'] == 'completed' and not observed[-1].get('terminal')
            monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'false')
            if planner:
                update('task_started')
                expect(card.locator('.run-activity')).to_have_count(0)
                expect(card).to_contain_text('確認')
            monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
            assert owner.deliver_fn(conversation, 'Planner: Sequential Mission\n  ◐ 隠す',
                is_status=True,group_id='hidden',status_kind='task_summary') is True
            expect(page.locator('.task-status-bubble[data-task-group-id="hidden"]')).to_have_count(0)
            foreign = 'Foreign conversation final must stay private.'
            assert owner.deliver_fn('web:foreign', foreign, is_final=True, group_id='foreign-group') is True
            final = '1. 再試行\n2. 別の方法で続ける'
            assert owner.deliver_fn(conversation, final, is_final=True, group_id='group') is True
            expect(page.locator('#messages ol li').filter(has_text='再試行')).to_have_count(1)
            expect(page.locator('#messages ol li').filter(has_text='別の方法で続ける')).to_have_count(1)
            history = page.request.get(server.base_url+'/api/chat/history/'+conversation).json()
            assert any(final == row.get('text') for row in history['messages'])
            expect(page.locator('#messages')).not_to_contain_text(foreign)
            assert not errors
        finally:
            browser.close();pw.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize('planner,verbose', [(False,False),(False,True),(True,False),(True,True)])
async def test_telegram_aggregator_queue_flushes_current_status_and_confirms_final(tmp_path, monkeypatch, planner, verbose):
    from dataclasses import replace
    from urllib.parse import parse_qs
    import httpx
    from nullion import telegram_app
    from nullion.config import load_settings
    from nullion.messaging_adapters import list_platform_delivery_receipts
    from nullion.mini_agent_runner import ProgressUpdate
    from nullion.platform_activity import PlatformTaskCardStore, platform_activity_capabilities
    from nullion.preferences import save_preferences
    from nullion.result_aggregator import ResultAggregator
    from nullion.run_activity import format_tool_activity_line
    from nullion.task_queue import TaskRegistry, TaskGroup, TaskRecord, TaskPriority, TaskStatus, TaskResult
    from nullion.tools import ToolExecutor, ToolInvocation, ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'emoji_level':'none', 'markdown_style':'plain'})
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'task' if planner else 'off')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'true' if verbose else 'false')
    wire = TelegramWire()
    original = wire.handle
    status_edits = []
    async def handle(request):
        method = request.url.path.rsplit('/',1)[-1]
        if method == 'editMessageText':
            fields = parse_qs((await request.aread()).decode())
            status_edits.append(fields['text'][0])
            return httpx.Response(200, json={'ok':True,'result':{'message_id':int(fields['message_id'][0]),
                'date':1,'chat':{'id':77,'type':'private'},'text':fields['text'][0]}})
        if method == 'sendChatAction':
            return httpx.Response(200, json={'ok':True,'result':True})
        return await original(request)
    wire.handle = handle
    install_wire(monkeypatch, wire)
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77', 'NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    class Owner:
        def set_deliver_fn(self, function): self.deliver_fn = function
    class Application:
        def run_polling(self, **kwargs): pass
    owner = Owner()
    service = telegram_app.ChatOperatorService(runtime, TOKEN, '77', settings, agent_orchestrator=owner)
    service._task_card_store = PlatformTaskCardStore(replace(
        platform_activity_capabilities('telegram'), min_update_interval_seconds=.04))
    service.run_polling(application=Application())
    delivery_calls = []
    async def deliver(conversation, text, **kwargs):
        result = await owner.deliver_fn(conversation, text, **kwargs)
        delivery_calls.append((text, kwargs, result))
        return result
    registry = TaskRegistry()
    task = TaskRecord('task','group','telegram:77','telegram:77','確認','公開情報を確認',
        TaskStatus.RUNNING,TaskPriority.NORMAL,['opaque_read'],[])
    group = TaskGroup('group','telegram:77','公開情報を確認',[task],
        planner_metadata={'disposition':'sequential_mission','tasks':['確認']})
    await registry.add_group(group)
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, outbox_path=tmp_path/'outbox.json')
    queue = asyncio.Queue()
    runner = asyncio.create_task(aggregator.run(queue))
    async def push(kind, message=None, data=None):
        before = len(delivery_calls)
        queue.put_nowait(ProgressUpdate('agent','task','group',kind,message,data))
        async with asyncio.timeout(3):
            while len(delivery_calls) == before:
                await asyncio.sleep(.005)
    try:
        await push('task_started')
        native = ToolRegistry()
        private = '/private/provider/credentials.json SECRET-CONNECTOR-PAYLOAD'
        native.register(ToolSpec('opaque_read','Read selected public information',ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,False,5),lambda invocation:ToolResult(invocation.invocation_id,
                invocation.tool_name,'completed',{'summary':private,'value':17}))
        running = ToolResult('observed','opaque_read','running',{})
        await push('tool_activity',format_tool_activity_line(running),{'tool_status':'running'})
        if verbose:
            async with asyncio.timeout(2):
                while not any('opaque_read' in body for body in [*wire.texts,*status_edits]):
                    await asyncio.sleep(.005)
        result = ToolExecutor(store=runtime.store,registry=native).invoke(
            ToolInvocation('observed','opaque_read','telegram:77',{}))
        assert result.status == 'completed'
        await push('tool_activity',format_tool_activity_line(result),{'tool_status':result.status})
        await asyncio.sleep(.09)  # No next event can rescue the trailing update.
        status = '\n'.join([*wire.texts,*status_edits])
        assert ('PLANNER' in status) is planner
        assert ('ACTIVITY LIVE' in status) is verbose
        assert ('opaque_read' in status) is verbose
        if verbose:
            latest = status_edits[-1] if status_edits else wire.texts[-1]
            assert '✓' in latest and 'opaque_read' in latest and '→ opaque_read' not in latest
        assert private not in status and 'SECRET' not in status
        assert registry.get_task('task').status is TaskStatus.RUNNING
        await registry.update_task('task',status=TaskStatus.COMPLETE,
            result=TaskResult('task','success',output='検証した値は17です。'))
        await push('task_complete')
        async with asyncio.timeout(3):
            while group.planner_metadata.get('delivery_status') != 'completed':
                await asyncio.sleep(.005)
        assert group.planner_metadata['outcome_verified'] is True
        outbox = aggregator._outbox.get('group')
        assert outbox['status']=='delivered' and outbox['text_delivered'] is True
        receipts = list_platform_delivery_receipts(status='succeeded')
        assert len(receipts)==1 and receipts[0]['attachment_count']==0 and receipts[0]['message_id']
        assert '17' in wire.texts[-1] and sum('17' in value for value in wire.texts)==1
        wire_snapshot = (list(wire.texts),list(status_edits))
        await push('tool_activity','→ stale_after_final',{'tool_status':'running'})
        await asyncio.sleep(.09)
        assert (wire.texts,status_edits)==wire_snapshot
        assert not service._task_card_store._pending
        assert any(getattr(event,'event_type',None)=='tool.invocation.completed' for event in runtime.store.events)
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)

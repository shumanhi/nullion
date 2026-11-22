import asyncio
from dataclasses import replace
import pytest

from nullion.platform_activity import PlatformTaskCardStore, deliver_platform_task_status_update, platform_activity_capabilities


@pytest.mark.asyncio
@pytest.mark.parametrize('platform', ['telegram', 'slack', 'discord'])
async def test_trailing_tool_update_flushes_and_group_terminal_cancels_pending(platform):
    store = PlatformTaskCardStore(replace(platform_activity_capabilities(platform), min_update_interval_seconds=.03))
    visible = []
    messages, locks = {}, {}
    async def send(target, text):
        visible.append(text)
        return 1
    async def edit(handle, target, text):
        visible.append(text)
        return True
    async def update(kind, text, **kwargs):
        return await deliver_platform_task_status_update(target_id='target', group_id='group', text=text,
            status_kind=kind, activity_id='opaque-tool', activity_label='調査', task_card_store=store,
            status_messages=messages, status_locks=locks, planner_feed_enabled=True,
            include_activity=True, send_new=send, edit_existing=edit, **kwargs)
    await update('task_summary', 'Planner: Sequential Mission\nFor: 要約\n  ◐ 調査')
    await update('tool_activity', '✓ opaque_read')
    await asyncio.sleep(.06)
    assert '✓ opaque_read' in visible[-1]
    assert 'ACTIVITY LIVE' in visible[-1]
    # A tool finishing leaves the group's task active and permits later tools.
    await update('tool_activity', '✓ opaque_second')
    await update('task_summary', 'Planner: Sequential Mission\n  ☑ 調査', terminal=True)
    terminal_text = visible[-1]
    await update('tool_activity', '→ stale_after_terminal')
    await asyncio.sleep(.06)
    assert visible[-1] == terminal_text
    store.clear(target_id='target', group_id='group')
    count = len(visible)
    await update('task_summary', 'Planner: Sequential Mission\n  ◐ stale_after_clear')
    await asyncio.sleep(.06)
    assert len(visible) == count and not store._pending


@pytest.mark.asyncio
async def test_concurrent_status_rendering_uses_newest_state_after_send_lock():
    store = PlatformTaskCardStore(platform_activity_capabilities('web'))
    messages, locks, visible = {}, {}, []
    entered, release = asyncio.Event(), asyncio.Event()
    async def send(target, text):
        entered.set()
        await release.wait()
        visible.append(text)
        return 1
    async def edit(handle, target, text):
        visible.append(text)
        return True
    async def update(text):
        return await deliver_platform_task_status_update(target_id='target', group_id='group', text=text,
            status_kind='task_summary', activity_id='', activity_label='', task_card_store=store,
            status_messages=messages, status_locks=locks, planner_feed_enabled=True,
            include_activity=False, send_new=send, edit_existing=edit)
    first = asyncio.create_task(update('◐ 途中'))
    await entered.wait()
    second = asyncio.create_task(update('☑ 完了'))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)
    assert len(visible) == 2 and visible[0].endswith('途中') and visible[1] == '☑ 完了'


@pytest.mark.asyncio
@pytest.mark.parametrize('planner,verbose', [(False,False),(False,True),(True,False),(True,True)])
async def test_planner_and_activity_visibility_are_independent(planner, verbose):
    store = PlatformTaskCardStore(platform_activity_capabilities('web'))
    sent = []
    async def send(target, text):
        sent.append(text)
        return 1
    async def edit(handle, target, text):
        sent.append(text)
        return True
    for kind, text in [('task_summary','Planner: Sequential Mission\n  ◐ 調査'),('tool_activity','✓ opaque_tool')]:
        await deliver_platform_task_status_update(target_id='target',group_id='group',status_kind=kind,text=text,
            activity_id='tool',activity_label='調査',task_card_store=store,status_messages={},status_locks={},
            planner_feed_enabled=planner,include_activity=verbose,send_new=send,edit_existing=edit)
    visible = '\n'.join(sent)
    assert ('PLANNER' in visible) is planner
    assert ('opaque_tool' in visible) is verbose
    assert ('ACTIVITY LIVE' in visible) is verbose


def test_web_tool_activity_uses_shared_private_output_policy():
    from nullion.tools import ToolResult
    from nullion.web_app import _web_tool_result_detail
    private = '/private/internal/task.json credential-provider-payload'
    assert _web_tool_result_detail(ToolResult('i','opaque_tool','completed',{'summary':private})) is None
    assert _web_tool_result_detail(ToolResult('i','opaque_tool','failed',{},error=private)) is None
    assert _web_tool_result_detail(ToolResult('i','list_crons','completed',{'crons':[{'task':private}]})) == '1 scheduled task'


@pytest.mark.parametrize('terminal', [False, True])
def test_dispatch_terminal_fact_survives_shared_reply_metadata(terminal):
    from nullion.chat_operator import _metadata_chat_reply
    from nullion.messaging_turn_graph import _delivery_metadata_from_reply, PlatformChatResponse
    reply = _metadata_chat_reply('結果', mini_agent_dispatch=True, mini_agent_dispatch_terminal=terminal,
        task_group_id='group')
    metadata = _delivery_metadata_from_reply(reply)
    response = PlatformChatResponse(text=str(reply), **metadata)
    assert response.mini_agent_dispatch_terminal is terminal
    assert response.text == '結果'


def test_doctor_async_notice_uses_persistent_callback_owner_loop():
    from nullion.agent_orchestrator import AgentOrchestrator, _notify_long_running_tool_loop
    from nullion.tools import ToolResult
    orchestrator = AgentOrchestrator(model_client=object())
    loop = orchestrator._ensure_dispatcher_loop()
    observed = []
    async def deliver(conversation_id, text, **kwargs):
        observed.append(asyncio.get_running_loop())
        return True
    _notify_long_running_tool_loop(deliver, conversation_id='telegram:1',
        tool_results=[ToolResult('i','opaque_read','completed',{})], owner_loop=loop)
    assert observed == [loop]
    orchestrator.shutdown_dispatcher_sync()


def test_timed_out_doctor_notice_does_not_publish_after_terminal_reply():
    import time
    from nullion.agent_orchestrator import AgentOrchestrator, _notify_long_running_tool_loop
    from nullion.tools import ToolResult
    orchestrator = AgentOrchestrator(model_client=object())
    loop = orchestrator._ensure_dispatcher_loop()
    observed = []
    async def deliver(*args, **kwargs):
        await asyncio.sleep(.06)
        observed.append('late notice')
    _notify_long_running_tool_loop(deliver, conversation_id='telegram:1',
        tool_results=[ToolResult('i','opaque_read','completed',{})], owner_loop=loop, timeout_seconds=.01)
    time.sleep(.09)
    assert not observed
    orchestrator.shutdown_dispatcher_sync()

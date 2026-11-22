"""Multipart status ownership and retry decisions, independent of adapters."""
import asyncio
from types import SimpleNamespace

import httpx
import pytest

from nullion.delivery_acknowledgement import UnconfirmedDelivery, send_error_replay_safety
from nullion.platform_activity import PlatformStatusMessageParts, deliver_platform_status_parts, split_platform_status_text


@pytest.mark.asyncio
async def test_parts_grow_shrink_and_keep_scalar_compatibility():
    messages, visible, deleted, next_id = {}, {999:'unowned'}, [], 0
    async def send(part):
        nonlocal next_id
        next_id += 1
        visible[next_id] = part
        return next_id
    async def edit(handle, part): visible[handle] = part; return True
    async def delete(handle): deleted.append(handle); visible.pop(handle); return True
    async def update(parts):
        return await deliver_platform_status_parts(messages=messages,key=('target','group'),parts=parts,
            send_part=send,edit_part=edit,delete_part=delete)
    assert await update(['最初']) and messages[('target','group')] == 1
    assert await update(['更新','次','終わり'])
    assert isinstance(messages[('target','group')],PlatformStatusMessageParts)
    assert await update(['完了'])
    assert messages[('target','group')] == 1 and visible == {999:'unowned',1:'完了'}
    assert next_id == 3 and deleted == [3,2]


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',['rejected','unknown','cancelled'])
async def test_partial_append_retains_confirmed_handles_and_never_replays_uncertain(failure):
    messages, accepted, calls = {}, [], []
    first = True
    async def send(part):
        nonlocal first
        calls.append(part)
        if part == '第二' and first:
            first = False
            if failure == 'rejected':
                response=httpx.Response(400,request=httpx.Request('POST','https://example.test'))
                raise httpx.HTTPStatusError('rejected',request=response.request,response=response)
            accepted.append(part)
            if failure == 'cancelled': raise asyncio.CancelledError()
            raise httpx.ReadTimeout('accepted; reply lost')
        accepted.append(part)
        return len(accepted)
    async def edit(handle,part): return True
    async def delete(handle): pytest.fail('No unowned or uncertain message may be deleted')
    async def update():
        return await deliver_platform_status_parts(messages=messages,key=('target','group'),
            parts=['第一','第二','第三'],send_part=send,edit_part=edit,delete_part=delete)
    expected = {'rejected':httpx.HTTPStatusError,'cancelled':asyncio.CancelledError,'unknown':UnconfirmedDelivery}[failure]
    with pytest.raises(expected): await update()
    state=messages[('target','group')]
    assert state.handles == [1]
    if failure=='rejected':
        assert state.uncertain_index is None and await update()
        assert accepted == ['第一','第二','第三']
    else:
        assert state.uncertain_index == 1
        before=list(calls)
        with pytest.raises(UnconfirmedDelivery) as caught: await update()
        assert caught.value.message_ids == ('1',) and calls == before
        assert accepted == ['第一','第二']


@pytest.mark.asyncio
async def test_unknown_edit_retries_same_confirmed_ids_without_appending():
    messages, accepted, edits = {}, [], []
    fail = False
    async def send(part): accepted.append(part); return len(accepted)
    async def edit(handle,part):
        nonlocal fail
        edits.append((handle,part))
        if fail:
            fail=False
            raise httpx.ReadTimeout('edit acknowledgement lost')
        return True
    async def delete(handle): return True
    async def update(parts):
        return await deliver_platform_status_parts(messages=messages,key=('t','g'),parts=parts,
            send_part=send,edit_part=edit,delete_part=delete)
    await update(['A','B']); fail=True
    with pytest.raises(httpx.ReadTimeout): await update(['C','D'])
    assert messages[('t','g')].uncertain_index is None
    await update(['C','D'])
    assert accepted == ['A','B'] and edits == [(1,'C'),(1,'C'),(2,'D')]


def test_status_split_preserves_non_english_content_and_balanced_fences():
    text='PLANNER\nFor: 比較\n20 tareas\n'+''.join(f'\n{i:02d} '+('🧭東京'*40) for i in range(20))+'\n```text\n'+('公開🧭\n'*500)+'```'
    parts=split_platform_status_text(text,limit=1900)
    assert len(parts)>1 and all(len(part.encode('utf-16-le'))//2<=1900 for part in parts)
    assert all(part.count('```')%2 == 0 for part in parts)
    for index in range(20): assert f'{index:02d} ' in '\n'.join(parts)


@pytest.mark.parametrize('code,expected',[(400,'rejected'),(401,'rejected'),(403,'rejected'),(404,'rejected'),
    (413,'rejected'),(422,'rejected'),(429,'retry'),(408,'unknown'),(409,'unknown'),(500,'unknown'),(502,'unknown'),(503,'unknown'),(504,'unknown')])
def test_native_discord_http_status_uses_shared_replay_policy(code,expected):
    from discord import HTTPException
    error=HTTPException(SimpleNamespace(status=code,reason='fixture'),{'message':'fixture','code':0})
    assert send_error_replay_safety(error)==expected
    error.__cause__=httpx.ReadTimeout('response may have been accepted')
    assert send_error_replay_safety(error)=='unknown'


@pytest.mark.parametrize('status',[True,False,'400',400.0,{},[],None,99,600])
def test_non_http_status_metadata_is_not_a_replay_grant(status):
    error=RuntimeError('opaque')
    error.status=status
    error.response=SimpleNamespace(status=status)
    assert send_error_replay_safety(error)=='unknown'


@pytest.mark.asyncio
async def test_closed_card_retains_late_acknowledgement_without_appending():
    from nullion.platform_activity import PlatformTaskCardStore, platform_activity_capabilities, deliver_platform_task_status_update
    store=PlatformTaskCardStore(platform_activity_capabilities('telegram'))
    messages,locks,calls={}, {}, []
    accepted,release=asyncio.Event(),asyncio.Event()
    async def send(part):
        calls.append(part)
        accepted.set()
        await release.wait()
        return 37
    async def edit(handle,part):pytest.fail('No existing message')
    async def delete(handle):pytest.fail('No surplus owned message')
    async def publish(target,text):
        delivered=await deliver_platform_status_parts(messages=messages,key=('77','group'),parts=['一','二','三'],
            send_part=send,edit_part=edit,delete_part=delete)
        return messages[('77','group')] if delivered else None
    running=asyncio.create_task(deliver_platform_task_status_update(target_id='77',group_id='group',
        text='PLANNER\nFor: 比較\n3 tasks',status_kind='task_summary',activity_id='',activity_label='',
        task_card_store=store,status_messages=messages,status_locks=locks,planner_feed_enabled=True,
        include_activity=False,send_new=publish,edit_existing=edit))
    await accepted.wait()
    store.clear(target_id='77',group_id='group')
    release.set()
    assert not await running
    assert calls==['一'] and messages[('77','group')].handles==[37]


@pytest.mark.parametrize('state,expected',[
    (PlatformStatusMessageParts(),False),
    (PlatformStatusMessageParts(handles=[37],payloads=['first']),False),
    (PlatformStatusMessageParts(handles=[37],payloads=['first'],uncertain_index=1),False),
    (PlatformStatusMessageParts(handles=[37,None],payloads=['first',None],complete=True),False),
    (PlatformStatusMessageParts(handles=[37,38],payloads=['first','second'],complete=True),True),
    (37,True),
])
def test_incomplete_cards_do_not_suppress_primary_planner_reply(monkeypatch,state,expected):
    from nullion import telegram_app
    from nullion.runtime_store import RuntimeStore
    runtime=SimpleNamespace(store=RuntimeStore())
    reply='Planner: Single Turn • 1 task\nFor: 比較\n  ☐ 確認'
    runtime.store.add_conversation_event({'conversation_id':'telegram:77','event_type':'conversation.chat_turn',
        'assistant_reply':reply,'tool_results':[
            {'tool_name':'Planner','status':'completed','output':{'summary':'Single Turn • 1 task'}},
            {'tool_name':'Mini-Agents','status':'running','output':{'task_group_id':'group'}}]})
    assert telegram_app._planner_summary_reply_should_be_suppressed(runtime,chat_id='77',reply=reply,
        status_messages={('77','group'):state},status_texts={('77','group'):'PLANNER previous content',
            ('77','other'):'PLANNER unrelated group'}) is expected

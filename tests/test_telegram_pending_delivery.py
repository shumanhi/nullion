import asyncio
from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_finish_waits_for_unacknowledged_draft_and_edits_same_message():
    from nullion.telegram_app import _TelegramTextDeltaStreamer
    wire = []
    release = asyncio.Event()
    class Draft:
        async def edit_text(self, text, **kwargs):
            wire[0] = text
    class Message:
        text = '確認して'
        async def reply_text(self, text, **kwargs):
            wire.append(text)
            await release.wait()
            return Draft()
    streamer = _TelegramTextDeltaStreamer(loop=asyncio.get_running_loop(), message=Message())
    emit = asyncio.create_task(asyncio.to_thread(streamer.emit, '**完了**'))
    await emit  # emit's two-second wait expires; the SDK call is still pending.
    assert not streamer.has_sent_message and len(wire) == 1
    finishing = asyncio.create_task(streamer.finish('**完了**'))
    await asyncio.sleep(.01)
    assert not finishing.done()
    release.set()
    assert await finishing
    assert wire == ['<b>完了</b>']


@pytest.mark.asyncio
async def test_failed_pending_send_does_not_poison_finalization(monkeypatch):
    from nullion import telegram_app as t
    class Message:
        text = 'α'
        async def reply_text(self, text, **kwargs):
            from telegram.error import BadRequest
            raise BadRequest('Definite SDK rejection')
    streamer = t._TelegramTextDeltaStreamer(loop=asyncio.get_running_loop(), message=Message())
    await asyncio.to_thread(streamer.emit, 'α')
    assert not await streamer.finish('α')
    assert not streamer._pending_sends

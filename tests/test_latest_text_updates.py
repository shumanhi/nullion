import asyncio
import time
from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize('surface', ['telegram', 'slack', 'discord'])
async def test_slow_draft_transport_does_not_backpressure_tokens_or_reorder_final(surface):
    from nullion.telegram_app import _TelegramTextDeltaStreamer
    from nullion.slack_app import _SlackTextDeltaStreamer
    from nullion.discord_app import _DiscordTextDeltaStreamer
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def deliver(text=None, **kwargs):
        calls.append(text if text is not None else kwargs.get('text', kwargs.get('content')))
        entered.set()
        await release.wait()
        return SimpleNamespace(edit_text=deliver, edit=deliver, delete=deliver)
    loop = asyncio.get_running_loop()
    if surface == 'telegram':
        streamer = _TelegramTextDeltaStreamer(loop=loop, message=SimpleNamespace(reply_text=deliver))
    elif surface == 'discord':
        streamer = _DiscordTextDeltaStreamer(loop=loop, channel=SimpleNamespace(send=deliver))
    else:
        async def update(**kwargs):
            await deliver(**kwargs)
            return {'ok':True}
        streamer = _SlackTextDeltaStreamer(loop=loop, client=SimpleNamespace(chat_update=update), channel='x', ts='1')
    started = time.perf_counter()
    await asyncio.to_thread(streamer.emit, '初稿')
    await asyncio.wait_for(entered.wait(), .5)
    for _ in range(50):
        await asyncio.to_thread(streamer.emit, '追加')
    assert time.perf_counter()-started < .3
    finalizing = asyncio.create_task(streamer.finish('最終結果'))
    await asyncio.sleep(.01)
    assert not finalizing.done() and calls == ['初稿']
    release.set()
    assert await asyncio.wait_for(finalizing, .5)
    assert calls == ['初稿', '最終結果']

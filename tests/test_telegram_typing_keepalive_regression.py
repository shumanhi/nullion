from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize('pooled', [False, True])
async def test_cancellation_at_indicator_completion_never_restarts_keepalive(monkeypatch, pooled):
    from nullion import telegram_app
    target=None
    calls=[]
    async def completed_send(*args, **kwargs):
        calls.append(1)
        # Complete the send at the same boundary as its owner's cancellation.
        # wait_for can consume that cancellation when its child is already done.
        asyncio.get_running_loop().call_soon(target.cancel)
    class BotContext:
        async def __aenter__(self):return SimpleNamespace()
        async def __aexit__(self,*args):pass
    monkeypatch.setattr(telegram_app,'build_telegram_bot',lambda *a,**kw:BotContext())
    monkeypatch.setattr(telegram_app,'_send_typing_indicator',completed_send)
    monkeypatch.setattr(telegram_app,'_send_telegram_chat_typing_indicator',completed_send)
    coroutine=(telegram_app._run_telegram_chat_typing_keepalive('synthetic',chat_id='77',runtime=SimpleNamespace(),text='1')
               if pooled else telegram_app._run_typing_keepalive(SimpleNamespace(),runtime=SimpleNamespace(),text='1'))
    target=asyncio.create_task(coroutine)
    try:
        done,_=await asyncio.wait({target},timeout=.2)
        assert done and target.done(), 'Completion swallowed cancellation and kept typing alive'
        assert calls==[1]
    finally:
        if not target.done():target.cancel()
        await asyncio.gather(target,return_exceptions=True)


@pytest.mark.asyncio
async def test_cancel_status_typing_keepalive_from_group_cancels_task() -> None:
    from nullion import telegram_app

    async def keepalive() -> None:
        while True:
            await asyncio.sleep(60)

    chat_id = "100001"
    group_id = "grp-typing-test"
    task = asyncio.create_task(keepalive())
    typing_tasks: dict[tuple[str, str], asyncio.Task[None]] = {(chat_id, group_id): task}

    telegram_app._cancel_telegram_status_typing_for_group(
        typing_tasks,
        chat_id=chat_id,
        group_id=group_id,
    )
    await asyncio.sleep(0)

    assert (chat_id, group_id) not in typing_tasks
    assert task.cancelled() or task.done() or task.cancelling() > 0


@pytest.mark.asyncio
async def test_task_status_active_work_does_not_keep_platform_typing_alive(monkeypatch) -> None:
    from nullion import telegram_app

    chat_id = "100001"
    group_id = "grp-typing-refresh"
    starts: list[str] = []
    refresh_calls: list[str] = []

    async def fake_keepalive(*_args, **_kwargs):  # noqa: ANN001
        starts.append("started")
        try:
            while True:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise

    async def fake_status_message(*_args, **_kwargs):  # noqa: ANN001
        return None

    async def fake_typing_refresh(*_args, **_kwargs):  # noqa: ANN001
        refresh_calls.append("called")
        return None

    monkeypatch.setattr(telegram_app, "_run_telegram_chat_typing_keepalive", fake_keepalive)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_status_message", fake_status_message)
    monkeypatch.setattr(telegram_app, "_send_telegram_chat_typing_indicator_by_token", fake_typing_refresh)

    typing_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
    await telegram_app._send_or_edit_telegram_task_status_message(
        bot=SimpleNamespace(),
        status_messages={},
        chat_id=chat_id,
        group_id=group_id,
        text="Planner: Sequential mission\n→ Working on 2 tasks:\n  ◐ Search\n  ☐ Report",
        runtime=SimpleNamespace(),
        bot_token="token",
        typing_tasks=typing_tasks,
    )
    await asyncio.sleep(0)

    assert starts == []
    assert refresh_calls == []
    assert typing_tasks == {}


@pytest.mark.asyncio
async def test_activity_streamer_can_finish_without_refreshing_typing(monkeypatch) -> None:
    from nullion import telegram_app

    typing_calls: list[str | None] = []

    async def fake_typing_indicator(_message, *, runtime, text):  # noqa: ANN001
        del runtime
        typing_calls.append(text)

    class StatusMessage:
        def __init__(self) -> None:
            self.edits: list[str] = []

        async def edit_text(self, text, **_kwargs):  # noqa: ANN001
            self.edits.append(text)

        async def delete(self):
            return None

    class Message:
        def __init__(self) -> None:
            self.status = StatusMessage()

        async def reply_text(self, text, **_kwargs):  # noqa: ANN001
            self.status.edits.append(text)
            return self.status

    monkeypatch.setattr(telegram_app, "_send_typing_indicator", fake_typing_indicator)

    streamer = telegram_app._TelegramActivityStreamer(
        Message(),
        runtime=SimpleNamespace(),
        typing_text="foreground request",
    )
    await streamer._update({"id": "orchestrate", "label": "Running tools", "status": "running", "detail": "tool"})
    streamer.disable_typing_refresh()
    await streamer.finish()

    assert typing_calls == ["foreground request"]

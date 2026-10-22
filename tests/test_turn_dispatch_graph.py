from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker, TurnDispatchPolicy, route_turn_dispatch


def test_turn_dispatch_runs_independent_messages_in_parallel() -> None:
    decision = route_turn_dispatch(
        "search cnn.com for technology news and send a pdf",
        active_turn_ids=("turn:previous",),
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.disposition is ConversationTurnDisposition.INDEPENDENT


def test_turn_dispatch_runs_short_additive_messages_in_parallel_without_language_heuristics() -> None:
    decision = route_turn_dispatch(
        "add images too",
        active_turn_ids=("turn:first", "turn:latest"),
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.disposition is ConversationTurnDisposition.INDEPENDENT


def test_turn_dispatch_runs_additive_standalone_request_in_parallel() -> None:
    decision = route_turn_dispatch(
        "And weather for tomorrow",
        active_turn_ids=("turn:first",),
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.disposition is ConversationTurnDisposition.INDEPENDENT


@pytest.mark.parametrize("text", ["where did you find this cron?", "cwhere did you find this cron?"])
def test_turn_dispatch_does_not_parse_referential_questions_with_hard_coded_language(text: str) -> None:
    decision = route_turn_dispatch(
        text,
        active_turn_ids=("turn:first", "turn:latest"),
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.disposition is ConversationTurnDisposition.INDEPENDENT
    assert decision.reason == "no_structured_dispatch_decision"


def test_turn_dispatch_has_no_dependency_without_active_turn() -> None:
    decision = route_turn_dispatch("add images too", active_turn_ids=())

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()


@pytest.mark.asyncio
async def test_async_turn_dispatch_tracker_runs_independent_turns_without_waiting() -> None:
    tracker = AsyncTurnDispatchTracker()
    events: list[str] = []

    async def first_turn() -> None:
        registration = await tracker.register("chat:1", "make me a pdf", turn_id="turn:first")
        async with registration:
            events.append("first:start")
            await asyncio.sleep(0.02)
            events.append("first:end")

    async def follow_up_turn() -> None:
        await asyncio.sleep(0.001)
        registration = await tracker.register("chat:1", "add images too", turn_id="turn:second")
        async with registration:
            events.append("second:start")

    await asyncio.gather(first_turn(), follow_up_turn())

    assert events == ["first:start", "second:start", "first:end"]


@pytest.mark.asyncio
async def test_async_turn_dispatch_tracker_cancels_active_session_tasks() -> None:
    tracker = AsyncTurnDispatchTracker()
    started = asyncio.Event()
    cancelled = asyncio.Event()
    registrations = []

    async def active_turn() -> None:
        registration = await tracker.register("chat:stop", "make reports", turn_id="turn:active")
        registrations.append(registration)
        async with registration:
            started.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.set()
                raise

    task = asyncio.create_task(active_turn())
    await asyncio.wait_for(started.wait(), timeout=1)

    cancelled_turn_ids = await tracker.cancel_conversation("chat:stop")

    assert cancelled_turn_ids == ("turn:active",)
    assert registrations[0].is_cancelled() is True
    await asyncio.wait_for(cancelled.wait(), timeout=1)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await tracker.active_turn_ids("chat:stop") == ()


def test_platform_adapters_use_shared_turn_dispatch_tracker() -> None:
    root = Path(
        os.environ.get(
            "NULLION_APP_REPO",
            Path(__file__).resolve().parents[1],
        )
    ).resolve()
    for path in [
        root / "src/nullion/telegram_app.py",
        root / "src/nullion/slack_app.py",
        root / "src/nullion/discord_app.py",
        root / "src/nullion/web_app.py",
    ]:
        source = path.read_text(encoding="utf-8")
        assert "turn_dispatch" in source

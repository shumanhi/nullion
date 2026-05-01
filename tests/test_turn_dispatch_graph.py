from __future__ import annotations

import asyncio
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


def test_turn_dispatch_waits_for_latest_active_follow_up() -> None:
    decision = route_turn_dispatch(
        "add images too",
        active_turn_ids=("turn:first", "turn:latest"),
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("turn:latest",)
    assert decision.disposition in {
        ConversationTurnDisposition.CONTINUE,
        ConversationTurnDisposition.REVISE,
        ConversationTurnDisposition.INTERRUPT,
        ConversationTurnDisposition.BACKGROUND_FOLLOW_UP,
    }


def test_turn_dispatch_runs_additive_standalone_request_in_parallel() -> None:
    decision = route_turn_dispatch(
        "And weather for tomorrow",
        active_turn_ids=("turn:first",),
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.disposition is ConversationTurnDisposition.INDEPENDENT


def test_turn_dispatch_has_no_dependency_without_active_turn() -> None:
    decision = route_turn_dispatch("add images too", active_turn_ids=())

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()


@pytest.mark.asyncio
async def test_async_turn_dispatch_tracker_waits_follow_up_for_active_turn() -> None:
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

    assert events == ["first:start", "first:end", "second:start"]


def test_platform_adapters_use_shared_turn_dispatch_tracker() -> None:
    root = Path(__file__).resolve().parents[1]
    for path in [
        root / "src/nullion/telegram_app.py",
        root / "src/nullion/slack_app.py",
        root / "src/nullion/discord_app.py",
        root / "src/nullion/web_app.py",
    ]:
        source = path.read_text(encoding="utf-8")
        assert "turn_dispatch" in source

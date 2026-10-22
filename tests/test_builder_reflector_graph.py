from __future__ import annotations

from datetime import UTC, datetime

from nullion.builder_observer import TaskCategory, TurnOutcome, TurnSignal
from nullion.builder_reflector import reflect_on_turn


def test_builder_reflector_runs_turn_reflection_as_graph() -> None:
    class Client:
        def create(self, **kwargs):
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"should_propose": true, "title": "Fetch report", '
                            '"trigger": "Use when fetching and saving reports", '
                            '"steps": ["Fetch source", "Write artifact"], '
                            '"tags": ["fetch"], "confidence": 0.8}'
                        ),
                    }
                ]
            }

    signal = TurnSignal(
        signal_id="sig",
        recorded_at=datetime(2026, 1, 1, tzinfo=UTC),
        user_message="fetch report",
        tool_names=("web_fetch", "file_write"),
        tool_count=2,
        outcome=TurnOutcome.SUCCESS,
        category=TaskCategory.MIXED,
        assistant_reply_length=20,
        had_approval_request=False,
        had_tool_errors=False,
        conversation_id="c",
    )

    result = reflect_on_turn(model_client=Client(), user_message="fetch report", assistant_reply="done", turn_signal=signal)

    assert result.should_propose is True
    assert result.proposal is not None
    assert result.proposal.title == "Fetch report"


def test_builder_reflector_graph_precheck_skips_single_tool_turn() -> None:
    class Client:
        def create(self, **kwargs):
            raise AssertionError("single-tool turns should not call the model")

    signal = TurnSignal(
        signal_id="sig",
        recorded_at=datetime(2026, 1, 1, tzinfo=UTC),
        user_message="fetch",
        tool_names=("web_fetch",),
        tool_count=1,
        outcome=TurnOutcome.SUCCESS,
        category=TaskCategory.FETCH,
        assistant_reply_length=5,
        had_approval_request=False,
        had_tool_errors=False,
        conversation_id="c",
    )

    result = reflect_on_turn(model_client=Client(), user_message="fetch", assistant_reply="done", turn_signal=signal)

    assert result.should_propose is False

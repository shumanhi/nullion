from datetime import UTC, datetime

from nullion.builder_observer import PatternSignal, TaskCategory, TurnOutcome, extract_turn_signal, detect_patterns
from nullion.builder_reflector import reflect_on_pattern, reflect_on_turn


class ExplodingModel:
    def create(self, **kwargs):
        raise AssertionError("Builder reflector must not call the model")


def test_turn_reflection_requires_two_distinct_tools():
    signal = extract_turn_signal(
        signal_id="sig-1",
        user_message="check weather",
        assistant_reply="Forecast.",
        tool_names=["web_search", "web_search"],
        tool_error_count=0,
        outcome=TurnOutcome.SUCCESS,
        conversation_id="web:test",
    )

    result = reflect_on_turn(
        model_client=ExplodingModel(),
        user_message="check weather",
        assistant_reply="Forecast.",
        turn_signal=signal,
    )

    assert not result.should_propose
    assert result.proposal is None


def test_pattern_reflection_requires_two_distinct_tools():
    pattern = PatternSignal(
        pattern_id="pattern-web-search",
        tool_sequence=("web_search",),
        category=TaskCategory.SEARCH,
        occurrence_count=3,
        first_seen_at=datetime(2026, 5, 2, tzinfo=UTC),
        last_seen_at=datetime(2026, 5, 2, tzinfo=UTC),
        example_user_messages=("weather tomorrow",),
        confidence=0.9,
    )

    result = reflect_on_pattern(model_client=ExplodingModel(), pattern=pattern)

    assert not result.should_propose
    assert result.proposal is None


def test_pattern_detector_ignores_single_tool_sequences():
    signals = [
        extract_turn_signal(
            signal_id=f"sig-{index}",
            user_message=f"weather {index}",
            assistant_reply="Forecast.",
            tool_names=["web_search"],
            tool_error_count=0,
            outcome=TurnOutcome.SUCCESS,
            conversation_id="web:test",
            recorded_at=datetime(2026, 5, 2, index, tzinfo=UTC),
        )
        for index in range(1, 4)
    ]

    assert detect_patterns(signals) == []

import asyncio
import json

import pytest

from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.turn_dispatch_graph import (
    AsyncTurnDispatchTracker,
    TurnDispatchPolicy,
    route_turn_dispatch,
    route_turn_dispatch_with_context,
)


class StructuredDispatchModel:
    def __init__(
        self,
        *,
        relationship: str,
        confidence: float = 0.9,
        effect: str | None = None,
        target_index: int | None = None,
        target_is_explicit: bool | None = None,
        work_ack_recommended: bool = False,
    ) -> None:
        self.relationship = relationship
        self.confidence = confidence
        self.effect = effect
        self.target_index = target_index
        self.target_is_explicit = target_is_explicit
        self.work_ack_recommended = work_ack_recommended
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        payload = {
            "relationship": self.relationship,
            "confidence": self.confidence,
            "work_ack_recommended": self.work_ack_recommended,
        }
        if self.effect is not None:
            payload["effect"] = self.effect
        if self.target_index is not None:
            payload["target_index"] = self.target_index
        if self.target_is_explicit is not None:
            payload["target_is_explicit"] = self.target_is_explicit
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(payload),
                }
            ]
        }


def test_turn_dispatch_without_active_turn_runs_immediately():
    decision = route_turn_dispatch("n'importe quoi", active_turn_ids=())

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.reason == "no_active_turn"


def test_turn_dispatch_with_active_turn_fails_open_without_structured_model_decision():
    decision = route_turn_dispatch("actualmente en un archivo de texto", active_turn_ids=("turn-1",))

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.reason == "no_structured_dispatch_decision"


def test_turn_dispatch_waits_only_on_model_structured_follow_up():
    model = StructuredDispatchModel(relationship="follow_up")

    decision = route_turn_dispatch_with_context(
        "別の形式にしてください",
        active_turn_ids=("turn-1",),
        active_turn_texts=("https://example.com を .pdf で保存",),
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("turn-1",)
    assert decision.reason == "model_structured_follow_up"
    request_payload = json.loads(model.calls[0]["messages"][0]["content"][0]["text"])
    assert request_payload["active_requests"][0]["user_request"] == "https://example.com を .pdf で保存"
    assert request_payload["allowed_effects"] == ["continue", "revise", "interrupt"]


def test_turn_dispatch_does_not_attach_generic_compact_reply_without_structured_evidence():
    model = StructuredDispatchModel(relationship="follow_up")

    decision = route_turn_dispatch_with_context(
        "d'accord",
        active_turn_ids=("turn-1",),
        active_turn_texts=("vérifie mes emails",),
        active_turn_followup_evidence=False,
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.reason == "no_structured_dispatch_decision"
    assert len(model.calls) == 1


def test_turn_dispatch_accepts_validated_semantic_target_against_active_runtime_request():
    model = StructuredDispatchModel(relationship="follow_up", target_index=0, target_is_explicit=True)

    decision = route_turn_dispatch_with_context(
        "regenerate the attached report with the revised section",
        active_turn_ids=("turn-1",),
        active_turn_texts=("create a self-contained report artifact",),
        active_turn_followup_evidence=False,
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("turn-1",)
    assert decision.reason == "model_structured_follow_up"
    request_payload = json.loads(model.calls[0]["messages"][0]["content"][0]["text"])
    assert "target_is_explicit" in model.calls[0]["system"]
    assert request_payload["active_requests"][0]["user_request"] == "create a self-contained report artifact"


@pytest.mark.parametrize("message", ["Find me the best one on amazon", "Busca el mejor en esa tienda", "その店で一番いいものを探して"])
def test_plain_semantic_followup_links_without_url_filename_or_attachment(message):
    model = StructuredDispatchModel(relationship="follow_up", target_index=0, target_is_explicit=True)
    decision = route_turn_dispatch_with_context(
        message,
        active_turn_ids=("lookup",),
        active_turn_texts=("Is this on amazon?",),
        active_turn_followup_evidence=False,
        model_client=model,
    )
    assert decision.dependency_turn_ids == ("lookup",)
    assert decision.disposition.value == "continue"
    assert len(model.calls) == 1


@pytest.mark.parametrize("target,explicit,effect", [(None, True, "continue"), (False, True, "continue"), ("0", True, "continue"), (0, "false", "continue"), (0, True, "invalid")])
def test_plain_followup_rejects_missing_or_malformed_structured_target(target, explicit, effect):
    model = StructuredDispatchModel(relationship="follow_up", target_index=target, target_is_explicit=explicit, effect=effect)
    decision = route_turn_dispatch_with_context(
        "opaque continuation",
        active_turn_ids=("lookup",),
        active_turn_texts=("opaque active request",),
        active_turn_followup_evidence=False,
        model_client=model,
    )
    assert decision.dependency_turn_ids == ()
    assert decision.policy is TurnDispatchPolicy.PARALLEL


def test_turn_dispatch_single_active_structured_evidence_falls_back_to_active_turn():
    class EmptyStructuredDispatchModel:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            return {"content": [{"type": "text", "text": ""}]}

    model = EmptyStructuredDispatchModel()

    decision = route_turn_dispatch_with_context(
        "opaque artifact revision request",
        active_turn_ids=("turn-1",),
        active_turn_texts=("structured artifact work",),
        active_turn_followup_evidence=True,
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("turn-1",)
    assert decision.reason == "structured_active_followup_evidence"
    assert len(model.calls) == 1


def test_turn_dispatch_active_request_structured_evidence_enables_compact_followup():
    class EmptyStructuredDispatchModel:
        def create(self, **_kwargs):
            return {"content": [{"type": "text", "text": ""}]}

    decision = route_turn_dispatch_with_context(
        "opaque compact continuation",
        active_turn_ids=("turn-1",),
        active_turn_texts=("create a file named stage_active_related_followup.html",),
        active_turn_followup_evidence=False,
        model_client=EmptyStructuredDispatchModel(),
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("turn-1",)
    assert decision.reason == "structured_active_followup_evidence"


def test_turn_dispatch_targets_model_selected_active_turn_among_parallel_requests():
    model = StructuredDispatchModel(relationship="follow_up", target_index=0)

    decision = route_turn_dispatch_with_context(
        "¿Eso requiere atención ahora?",
        active_turn_ids=("email-request", "market-report-request"),
        active_turn_texts=("メールの新着を確認する", "informe bursátil posterior al cierre"),
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("email-request",)
    assert decision.target_active_turn_index == 0
    assert decision.reason == "model_structured_follow_up"
    request_payload = json.loads(model.calls[0]["messages"][0]["content"][0]["text"])
    assert request_payload["allowed_target_indexes"] == [0, 1]


def test_turn_dispatch_can_target_later_parallel_request_without_latest_assumption():
    model = StructuredDispatchModel(relationship="follow_up", target_index=1)

    decision = route_turn_dispatch_with_context(
        "¿Ese informe terminó?",
        active_turn_ids=("email-request", "market-report-request"),
        active_turn_texts=("メールの新着を確認する", "informe bursátil posterior al cierre"),
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("market-report-request",)
    assert decision.target_active_turn_index == 1
    assert decision.reason == "model_structured_follow_up"


def test_turn_dispatch_fails_open_when_parallel_follow_up_has_no_structured_target():
    model = StructuredDispatchModel(relationship="follow_up")

    decision = route_turn_dispatch_with_context(
        "¿Eso requiere atención ahora?",
        active_turn_ids=("email-request", "market-report-request"),
        active_turn_texts=("メールの新着を確認する", "informe bursátil posterior al cierre"),
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.reason == "no_structured_dispatch_decision"


def test_turn_dispatch_fails_open_when_model_targets_unlisted_active_index():
    model = StructuredDispatchModel(relationship="follow_up", target_index=9)

    decision = route_turn_dispatch_with_context(
        "¿Eso requiere atención ahora?",
        active_turn_ids=("email-request", "market-report-request"),
        active_turn_texts=("メールの新着を確認する", "informe bursátil posterior al cierre"),
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.reason == "no_structured_dispatch_decision"


def test_turn_dispatch_marks_model_structured_revision():
    model = StructuredDispatchModel(relationship="follow_up", effect="revise")

    decision = route_turn_dispatch_with_context(
        "別の形式にしてください",
        active_turn_ids=("turn-1",),
        active_turn_texts=("https://example.com を .pdf で保存",),
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
    assert decision.dependency_turn_ids == ("turn-1",)
    assert decision.disposition is ConversationTurnDisposition.REVISE
    assert decision.reason == "model_structured_revision"


def test_turn_dispatch_runs_parallel_on_model_structured_separate_request():
    model = StructuredDispatchModel(relationship="separate")

    decision = route_turn_dispatch_with_context(
        "mañana tiempo 10024",
        active_turn_ids=("turn-1",),
        active_turn_texts=("https://example.com を .pdf で保存",),
        model_client=model,
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.reason == "model_structured_separate"
    assert decision.work_ack_recommended is False


def test_turn_dispatch_ignores_model_work_ack_prediction_for_separate_turns():
    work_model = StructuredDispatchModel(relationship="separate", work_ack_recommended=True)
    chatter_model = StructuredDispatchModel(relationship="separate", work_ack_recommended=False)

    work_decision = route_turn_dispatch_with_context(
        "¿Cuál es el clima ahora?",
        active_turn_ids=("turn-1",),
        active_turn_texts=("https://example.com を .pdf で保存",),
        model_client=work_model,
    )
    chatter_decision = route_turn_dispatch_with_context(
        "hola, ¿cómo estás?",
        active_turn_ids=("turn-1",),
        active_turn_texts=("https://example.com を .pdf で保存",),
        model_client=chatter_model,
    )

    assert work_decision.policy is TurnDispatchPolicy.PARALLEL
    assert work_decision.reason == "model_structured_separate"
    assert work_decision.work_ack_recommended is False
    assert chatter_decision.policy is TurnDispatchPolicy.PARALLEL
    assert chatter_decision.reason == "model_structured_separate"
    assert chatter_decision.work_ack_recommended is False


def test_turn_dispatch_ignores_conjunction_like_text_without_structured_model_follow_up():
    decision = route_turn_dispatch(
        "build the spreadsheet and send a note when the uploaded .xlsx is ready",
        active_turn_ids=("turn-1",),
    )

    assert decision.policy is TurnDispatchPolicy.PARALLEL
    assert decision.dependency_turn_ids == ()
    assert decision.reason == "no_structured_dispatch_decision"


@pytest.mark.asyncio
async def test_turn_dispatch_tracker_serializes_same_conversation_from_structured_model_decision():
    tracker = AsyncTurnDispatchTracker()
    first_continue = asyncio.Event()
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    model = StructuredDispatchModel(relationship="follow_up")

    async def first_turn():
        registration = await tracker.register("telegram:1", "https://example.com を .pdf で保存", turn_id="first")
        async with registration:
            first_started.set()
            await first_continue.wait()

    async def second_turn():
        await first_started.wait()
        registration = await tracker.register(
            "telegram:1",
            "別の形式にしてください",
            turn_id="second",
            model_client=model,
            active_turn_followup_evidence=True,
        )
        assert registration.decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
        async with registration:
            second_started.set()

    first_task = asyncio.create_task(first_turn())
    second_task = asyncio.create_task(second_turn())
    await first_started.wait()
    await asyncio.sleep(0)

    assert not second_started.is_set()

    first_continue.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=1)
    assert second_started.is_set()


@pytest.mark.asyncio
async def test_turn_dispatch_tracker_marks_revised_active_turn_superseded():
    tracker = AsyncTurnDispatchTracker()
    first_continue = asyncio.Event()
    first_started = asyncio.Event()
    first_superseded = asyncio.Event()
    second_started = asyncio.Event()
    model = StructuredDispatchModel(relationship="follow_up", effect="revise")

    async def first_turn():
        registration = await tracker.register("telegram:1", "https://example.com を .pdf で保存", turn_id="first")
        async with registration:
            first_started.set()
            while not await registration.is_superseded():
                await asyncio.sleep(0)
            first_superseded.set()
            await first_continue.wait()

    async def second_turn():
        await first_started.wait()
        registration = await tracker.register(
            "telegram:1",
            "別の形式にしてください",
            turn_id="second",
            model_client=model,
            active_turn_followup_evidence=True,
        )
        assert registration.decision.disposition is ConversationTurnDisposition.REVISE
        async with registration:
            second_started.set()

    first_task = asyncio.create_task(first_turn())
    second_task = asyncio.create_task(second_turn())
    await first_started.wait()
    await asyncio.wait_for(first_superseded.wait(), timeout=1)
    assert not second_started.is_set()

    first_continue.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=1)
    assert second_started.is_set()


@pytest.mark.asyncio
async def test_turn_dispatch_tracker_runs_separate_structured_request_in_parallel():
    tracker = AsyncTurnDispatchTracker()
    first_continue = asyncio.Event()
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    model = StructuredDispatchModel(relationship="separate")

    async def first_turn():
        registration = await tracker.register("telegram:1", "https://example.com を .pdf で保存", turn_id="first")
        async with registration:
            first_started.set()
            await first_continue.wait()

    async def second_turn():
        await first_started.wait()
        registration = await tracker.register(
            "telegram:1",
            "mañana tiempo 10024",
            turn_id="second",
            model_client=model,
            active_turn_followup_evidence=True,
        )
        assert registration.decision.policy is TurnDispatchPolicy.PARALLEL
        async with registration:
            second_started.set()

    first_task = asyncio.create_task(first_turn())
    second_task = asyncio.create_task(second_turn())
    await first_started.wait()
    await asyncio.wait_for(second_started.wait(), timeout=1)

    first_continue.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=1)


@pytest.mark.asyncio
async def test_turn_dispatch_tracker_uses_structured_model_for_active_followup():
    tracker = AsyncTurnDispatchTracker()
    first_continue = asyncio.Event()
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    model = StructuredDispatchModel(relationship="follow_up", target_is_explicit=True)

    async def first_turn():
        registration = await tracker.register("telegram:1", "メールの新着を確認する", turn_id="first")
        async with registration:
            first_started.set()
            await first_continue.wait()

    async def second_turn():
        await first_started.wait()
        registration = await tracker.register(
            "telegram:1",
            "regenerate the report with the updated attachment",
            turn_id="second",
            model_client=model,
            active_turn_followup_evidence=True,
        )
        assert registration.decision.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE
        assert registration.decision.reason == "model_structured_follow_up"
        second_started.set()
        async with registration:
            pass

    first_task = asyncio.create_task(first_turn())
    second_task = asyncio.create_task(second_turn())
    await first_started.wait()
    await asyncio.wait_for(second_started.wait(), timeout=1)

    first_continue.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=1)
    assert len(model.calls) == 1


@pytest.mark.parametrize("message", ["Update that with the new requirements", "その条件に変更して", "Cámbialo con esos requisitos"])
def test_reasoning_budget_preserves_complete_structured_followup(message):
    class BudgetedModel(StructuredDispatchModel):
        def create(self, **kwargs):
            if kwargs.get("max_tokens", 0) < 512:
                self.calls.append(kwargs)
                return {"completion": {"status": "incomplete", "reason": "max_output_tokens"},
                        "content": [{"type": "text", "text": '{"relationship":"follow_up",'}]}
            return super().create(**kwargs)
    model = BudgetedModel(relationship="follow_up", target_index=0, target_is_explicit=True, effect="revise")
    result = route_turn_dispatch_with_context(message, active_turn_ids=("current",),
        active_turn_texts=("Compare current listings",), active_turn_followup_evidence=False, model_client=model)
    assert result.disposition is ConversationTurnDisposition.REVISE
    assert result.dependency_turn_ids == ("current",)
    assert len(model.calls) == 1
    idle = route_turn_dispatch_with_context(message, active_turn_ids=(), active_turn_texts=(), model_client=model)
    assert idle.dependency_turn_ids == ()
    assert len(model.calls) == 1, "Idle turns must not enter the relationship classifier"

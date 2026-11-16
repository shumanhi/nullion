import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json

import pytest

from nullion.conversation_runtime import ConversationBranch, ConversationBranchStatus, ConversationTurn, ConversationTurnDisposition
from nullion.runtime_store import RuntimeStore
from nullion.turn_dispatch_choices import dispatch_choice_question, record_dispatch_choice, selected_dispatch_choice
from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker, TurnDispatchDecision, TurnDispatchPolicy, route_turn_dispatch_with_context


@pytest.mark.parametrize('options', ['1. 現在の注文\n2. 変更を追跡', '1. Pedidos actuales\n2. Vigilar cambios'])
def test_numbered_question_binds_without_concurrent_classifier(options):
    store = RuntimeStore()
    add_target(store, 'question', '注文')
    store.add_conversation_event({'event_type':'conversation.chat_turn', 'conversation_id':'chat:1',
        'turn_id':'question', 'assistant_reply':options})
    class Model:
        def create(self, **kwargs): pytest.fail('Numeric UI selection reached relationship inference')
    async def replay():
        tracker = AsyncTurnDispatchTracker()
        other = await tracker.register('chat:1', '別の作業', turn_id='other')
        selected = await tracker.register('chat:1', '1', turn_id='selected', model_client=Model(), runtime_store=store)
        assert selected.decision.dependency_turn_ids == ('question',)
        assert selected.decision.reason == 'numbered_conversation_choice'
        assert selected.decision.policy is TurnDispatchPolicy.PARALLEL
        await selected.finish(); await other.finish()
    asyncio.run(replay())


def test_completed_publication_releases_waiter_before_transport_cleanup():
    async def replay():
        tracker = AsyncTurnDispatchTracker()
        ready, finish, cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def first():
            reg = await tracker.register('chat:1', '原稿', turn_id='first', finish_after_processing=False)
            ready.set(); await finish.wait(); await reg.finish(); await cleanup.wait()
        task = asyncio.create_task(first()); await ready.wait()
        class Model:
            def create(self, **kwargs):
                return {'content':[{'type':'text','text':json.dumps({'relationship':'follow_up','effect':'continue',
                    'confidence':1,'target_index':0,'target_is_explicit':True})}]}
        follow = await tracker.register('chat:1', 'その原稿を保存', turn_id='follow', model_client=Model())
        waiter = asyncio.create_task(follow.wait_for_dependencies())
        await asyncio.sleep(0); assert not waiter.done()
        finish.set(); await asyncio.wait_for(waiter, .5)
        assert not task.done(), 'The test must retain slow post-delivery cleanup'
        assert await tracker.active_turn_ids('chat:1') == ('follow',)
        await follow.finish(); cleanup.set(); await task
        assert not tracker._completion_by_conversation and not tracker._done_callbacks
    asyncio.run(replay())


def test_waiter_cancellation_cannot_cancel_predecessor():
    async def replay():
        tracker = AsyncTurnDispatchTracker()
        ready, release = asyncio.Event(), asyncio.Event()
        async def first():
            reg = await tracker.register('chat:1', '保存', turn_id='first')
            ready.set(); await release.wait(); await reg.finish()
        task = asyncio.create_task(first()); await ready.wait()
        model = type('Model', (), {'create':lambda self, **kw: {'content':[{'type':'text','text':json.dumps({
            'relationship':'follow_up','confidence':1,'effect':'continue','target_index':0,'target_is_explicit':True})}]}})()
        follow = await tracker.register('chat:1', '続ける', turn_id='follow', model_client=model)
        waiter = asyncio.create_task(follow.wait_for_dependencies()); await asyncio.sleep(0); waiter.cancel()
        with pytest.raises(asyncio.CancelledError): await waiter
        assert not task.cancelled() and not follow.dependency_tasks[0].cancelled()
        await follow.finish(); release.set(); await task
    asyncio.run(replay())


def test_finished_task_cannot_leave_orphan_active_registration():
    async def replay():
        tracker = AsyncTurnDispatchTracker()
        async def abandoned():
            await tracker.register('chat:1', 'bonjour', turn_id='greeting', finish_after_processing=False)
            # An adapter fails before entering its normal cleanup block.
        await asyncio.create_task(abandoned())
        for _ in range(3): await asyncio.sleep(0)
        assert await tracker.active_turn_ids('chat:1') == ()
        class Model:
            def create(self, **kw):pytest.fail('A completed task triggered relationship inference')
        follow = await tracker.register('chat:1', 'その画面', turn_id='follow', model_client=Model())
        assert follow.decision.reason == 'no_active_turn'
        await follow.finish()
        assert not tracker._done_callbacks
    asyncio.run(replay())


def test_ambiguity_only_lists_model_validated_plausible_targets_and_exposes_result_context():
    store = RuntimeStore()
    add_target(store, 'weather', 'Météo')
    add_target(store, 'breakfast', '朝食')
    add_target(store, 'tickets', 'Entradas')
    store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'chat:1',
        'turn_id':'tickets','assistant_reply':'Seat map at 6:00 PM',
        'tool_results':[{'tool_name':'browser_navigate','status':'completed'}]})
    class Model:
        def create(self, **kw):
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            assert len(packet['active_requests']) == 2, packet
            current = packet['active_requests'][1]
            assert current['last_reply'] == 'Seat map at 6:00 PM'
            assert current['completed_tools'] == ['browser_navigate']
            assert current['is_running'] is False
            return {'content':[{'type':'text','text':json.dumps({'relationship':'ambiguous','confidence':1,
                'effect':'continue','plausible_target_indexes':[1]})}]}
    async def replay():
        tracker = AsyncTurnDispatchTracker()
        greeting = await tracker.register('chat:1', 'Hi', turn_id='greeting')
        reg = await tracker.register('chat:1', 'Capture ceci', turn_id='new', runtime_store=store, model_client=Model())
        assert reg.decision.clarification_targets == (('tickets', 'Entradas'),)
        menu = dispatch_choice_question(reg.decision)
        assert 'Hi' not in menu and 'Météo' not in menu and '朝食' not in menu
        await reg.finish(); await greeting.finish()
    asyncio.run(replay())


def add_target(store, identity, message, conversation="chat:1"):
    branch = ConversationBranch(identity, conversation, ConversationBranchStatus.ACTIVE, identity)
    store.add_conversation_branch(branch)
    store.add_conversation_turn(ConversationTurn(identity, conversation, identity, None,
        ConversationTurnDisposition.INDEPENDENT, message, "completed", datetime.now(timezone.utc)))
    store.add_conversation_event({"event_type": "conversation.chat_turn", "conversation_id": conversation,
        "turn_id": identity, "user_message": message, "assistant_reply": "Result"})


def pending(store):
    decision = TurnDispatchDecision(TurnDispatchPolicy.PARALLEL, reason="ambiguous_dispatch_target",
        clarification_targets=(("a", "Source inspection"), ("b", "Invitation")),
        clarification_effect=ConversationTurnDisposition.REVISE)
    record_dispatch_choice(store, "chat:1", "question", "修正して", decision)
    store.add_conversation_event({"event_type": "conversation.chat_turn", "conversation_id": "chat:1",
        "turn_id": "question", "assistant_reply": dispatch_choice_question(decision)})
    return decision


@pytest.mark.parametrize("message", ["Fix it", "Corrígelo", "修正して", "ठीक करो"])
@pytest.mark.parametrize("structured_evidence", [False, True])
def test_vague_reference_never_cancels_a_task_even_with_a_url_or_artifact(message, structured_evidence):
    class Model:
        def create(self, **kwargs):
            return {"content": [{"type": "text", "text": json.dumps({"relationship": "follow_up",
                "effect": "revise", "confidence": .99, "target_index": 0, "target_is_explicit": False})}]}
    decision = route_turn_dispatch_with_context(message, active_turn_ids=("a",),
        active_turn_texts=("Inspect https://example.test/board",),
        active_turn_followup_evidence=structured_evidence, model_client=Model())
    assert not decision.dependency_turn_ids
    assert not decision.should_wait
    assert "1." in dispatch_choice_question(decision)


def test_numeric_choice_is_bound_to_original_message_and_only_selected_task():
    store = RuntimeStore()
    add_target(store, "a", "Source inspection")
    add_target(store, "b", "Invitation")
    pending(store)
    decision = selected_dispatch_choice(store, "chat:1", "2")
    assert decision.dependency_turn_ids == ("b",)
    assert decision.resolved_message == "修正して"
    assert decision.disposition is ConversationTurnDisposition.REVISE
    assert selected_dispatch_choice(store, "chat:1", "2") is None


def test_invalid_number_repeats_menu_without_consuming_or_changing_targets():
    store = RuntimeStore()
    add_target(store, "a", "Source inspection")
    add_target(store, "b", "Invitation")
    pending(store)
    decision = selected_dispatch_choice(store, "chat:1", "9")
    assert not decision.dependency_turn_ids
    assert decision.resolved_message == "修正して"
    assert "1." in dispatch_choice_question(decision)
    assert selected_dispatch_choice(store, "chat:1", "2").dependency_turn_ids == ("b",)


def test_late_concurrent_result_cannot_steal_a_numeric_target_choice():
    store = RuntimeStore()
    add_target(store, "a", "Source inspection")
    add_target(store, "b", "Invitation")
    pending(store)
    store.add_conversation_turn(ConversationTurn("question", "chat:1", "menu", None,
        ConversationTurnDisposition.INDEPENDENT, "Fix it", "completed", datetime.now(timezone.utc)))
    store.add_conversation_event({"event_type":"conversation.chat_turn", "conversation_id":"chat:1",
        "turn_id":"a", "assistant_reply":"Older work has finished"})
    decision = selected_dispatch_choice(store, "chat:1", "2")
    assert not decision.dependency_turn_ids
    assert dispatch_choice_question(decision)
    assert decision.resolved_message == "修正して"


def test_no_pending_choice_uses_index_without_scanning_history(monkeypatch):
    store = RuntimeStore()
    store.add_conversation_event({"event_type":"conversation.chat_turn", "conversation_id":"chat:1"})
    monkeypatch.setattr(RuntimeStore, "list_recent_conversation_events_after_reset",
        lambda *a, **kw: pytest.fail("Plain ingress scanned history for an absent menu"))
    assert selected_dispatch_choice(store, "chat:1", "1") is None


def test_choice_index_survives_rebuild_and_honors_reset():
    store = RuntimeStore()
    add_target(store, "a", "Source inspection")
    add_target(store, "b", "Invitation")
    pending(store)
    restored = RuntimeStore(conversation_events=list(store.conversation_events),
        conversation_turns=dict(store.conversation_turns), conversation_branches=dict(store.conversation_branches))
    assert selected_dispatch_choice(restored, "chat:1", "2").dependency_turn_ids == ("b",)
    store.add_conversation_event({"event_type":"conversation.session_reset", "conversation_id":"chat:1"})
    assert store.latest_conversation_event_after_reset("chat:1", "conversation.dispatch_choice") is None


@pytest.mark.parametrize("mode", ["timeout", "malformed", "bad_effect", "nan_confidence"])
def test_unavailable_or_malformed_classifier_asks_without_guessing(mode):
    class Model:
        def create(self, **kwargs):
            if mode == "timeout": raise TimeoutError("external provider")
            packet = {"relationship":"follow_up", "target_is_explicit":False,
                "effect":{} if mode == "bad_effect" else "revise", "confidence":"NaN" if mode == "nan_confidence" else 1}
            return {"content":[{"type":"text", "text":"[" if mode == "malformed" else json.dumps(packet)}]}
    decision = route_turn_dispatch_with_context("修正して", active_turn_ids=("a",),
        active_turn_texts=("a request",), model_client=Model())
    assert not decision.dependency_turn_ids
    assert dispatch_choice_question(decision)


@pytest.mark.parametrize("invalidator", ["other_conversation", "other_reply", "newer_message", "reset", "cancelled"])
def test_choices_cannot_cross_conversations_replies_resets_or_cancellations(invalidator):
    store = RuntimeStore()
    add_target(store, "a", "Source inspection")
    add_target(store, "b", "Invitation")
    pending(store)
    context = None
    conversation = "chat:1"
    if invalidator == "other_conversation": conversation = "chat:2"
    if invalidator == "other_reply": context = {"reply_to_turn_id": "a"}
    if invalidator == "newer_message":
        store.add_conversation_event({"event_type": "conversation.chat_turn", "conversation_id": conversation,
            "turn_id": "newer", "assistant_reply": "1. A different question"})
    if invalidator == "reset":
        store.add_conversation_event({"event_type": "conversation.session_reset", "conversation_id": conversation})
    if invalidator == "cancelled":
        store.add_conversation_branch(replace(store.get_conversation_branch("b"), status=ConversationBranchStatus.CANCELLED))
    result = selected_dispatch_choice(store, conversation, "2", context)
    assert result is None or (not result.dependency_turn_ids and dispatch_choice_question(result))


@pytest.mark.asyncio
async def test_ambiguity_preserves_running_publication_and_includes_recent_request():
    store = RuntimeStore()
    add_target(store, "a", "Inspect aviation source")
    add_target(store, "b", "Generate invitation")
    class Model:
        def create(self, **kwargs):
            return {"content": [{"type": "text", "text": '{"relationship":"ambiguous","confidence":1}'}]}
    tracker = AsyncTurnDispatchTracker()
    parent = await tracker.register("chat:1", "Inspect aviation source", turn_id="a", runtime_store=store)
    question = await tracker.register("chat:1", "Fix it", turn_id="question", runtime_store=store, model_client=Model())
    assert {identity for identity, _ in question.decision.clarification_targets} == {"a", "b"}
    assert not parent.is_cancelled()
    assert not await parent.is_superseded()
    await question.finish()
    await parent.finish()

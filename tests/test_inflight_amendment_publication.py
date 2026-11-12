import asyncio
import threading
import time

import pytest

from nullion.conversation_runtime import ConversationTurnDisposition as Disposition
from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker, TurnDispatchDecision, TurnDispatchPolicy, TurnPublicationGate


@pytest.mark.asyncio
@pytest.mark.parametrize('effect', [Disposition.REVISE, Disposition.INDEPENDENT, Disposition.CONTINUE])
async def test_pending_relationship_decision_holds_publication_then_releases_only_valid_generation(monkeypatch, effect):
    import nullion.turn_dispatch_graph as dispatch
    tracker = AsyncTurnDispatchTracker()
    started, finish = asyncio.Event(), asyncio.Event()
    registration = None
    async def parent():
        nonlocal registration
        registration = await tracker.register('chat:1', 'α', turn_id='parent')
        started.set()
        await finish.wait()
        await registration.finish()
    origin = asyncio.create_task(parent())
    await started.wait()
    entered, release = threading.Event(), threading.Event()
    def classify(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return TurnDispatchDecision(TurnDispatchPolicy.PARALLEL if effect is Disposition.INDEPENDENT else TurnDispatchPolicy.WAIT_FOR_ACTIVE,
            () if effect is Disposition.INDEPENDENT else ('parent',), effect)
    monkeypatch.setattr(dispatch, 'route_turn_dispatch_with_context', classify)
    successor = asyncio.create_task(tracker.register('chat:1', 'β', turn_id='child'))
    assert await asyncio.to_thread(entered.wait, 3)
    publication = asyncio.create_task(registration.publication_gate.wait())
    await asyncio.sleep(.01)
    assert not publication.done()
    release.set()
    child = await successor
    assert await publication is (effect is not Disposition.REVISE)
    assert registration.is_cancelled() is (effect is Disposition.REVISE)
    finish.set()
    await origin
    assert await registration.is_superseded() is (effect is Disposition.REVISE), 'Revocation must survive registration cleanup'
    await child.finish()


@pytest.mark.asyncio
async def test_cancelled_classifier_does_not_leave_parent_publication_frozen(monkeypatch):
    import nullion.turn_dispatch_graph as dispatch
    tracker = AsyncTurnDispatchTracker()
    origin = await tracker.register('chat:1', 'α', turn_id='parent')
    entered, release = threading.Event(), threading.Event()
    def classify(*args, **kwargs):
        entered.set()
        release.wait(3)
        return TurnDispatchDecision(TurnDispatchPolicy.PARALLEL)
    monkeypatch.setattr(dispatch, 'route_turn_dispatch_with_context', classify)
    pending = asyncio.create_task(tracker.register('chat:1', 'β', turn_id='child'))
    assert await asyncio.to_thread(entered.wait, 3)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    release.set()
    assert await asyncio.wait_for(origin.publication_gate.wait(), 1)
    await origin.finish()


@pytest.mark.asyncio
async def test_rapid_revisions_target_the_newest_admitted_generation(monkeypatch):
    import nullion.turn_dispatch_graph as dispatch
    tracker = AsyncTurnDispatchTracker()
    stop = asyncio.Event()
    registrations = []
    async def task(turn):
        registration = await tracker.register('chat:1', turn, turn_id=turn)
        registrations.append(registration)
        await stop.wait()
        await registration.finish()
    origin = asyncio.create_task(task('α'))
    await asyncio.sleep(.001)
    targets = []
    def classify(*args, **kwargs):
        parent = kwargs['active_turn_ids'][-1]
        targets.append(parent)
        return TurnDispatchDecision(TurnDispatchPolicy.WAIT_FOR_ACTIVE, (parent,), Disposition.REVISE)
    monkeypatch.setattr(dispatch, 'route_turn_dispatch_with_context', classify)
    second, third = asyncio.create_task(task('β')), asyncio.create_task(task('γ'))
    while len(registrations) < 3:
        await asyncio.sleep(.001)
    assert targets == ['α', 'β']
    assert [r.publication_gate.can_publish() for r in registrations] == [False, False, True]
    stop.set()
    await asyncio.gather(origin, second, third)
    assert await tracker.active_turn_ids('chat:1') == ()


@pytest.mark.asyncio
async def test_plain_admission_keeps_fast_path_without_classifier(monkeypatch):
    tracker = AsyncTurnDispatchTracker()
    class SlowModel:
        def create(self, **kwargs):
            raise AssertionError('Ordinary non-overlapping turns must not invoke relationship classification')
    elapsed = []
    for index in range(10):
        start = time.perf_counter()
        registration = await tracker.register('chat:1', str(index), model_client=SlowModel())
        await registration.finish()
        elapsed.append(time.perf_counter() - start)
    assert sum(elapsed) / len(elapsed) < .025
    assert max(elapsed) < .1


@pytest.mark.asyncio
async def test_telegram_retries_do_not_publish_after_revision(monkeypatch):
    from nullion import telegram_app as telegram
    gate, calls = TurnPublicationGate(), []
    class Message:
        text = 'α'
        async def reply_text(self, text, **kwargs):
            calls.append(text)
            gate.revoke()
            raise TimeoutError('network timeout')
    streamer = telegram._TelegramTextDeltaStreamer(loop=asyncio.get_running_loop(), message=Message(), publication_gate=gate)
    assert not await streamer._send_or_edit('obsolete')
    assert not await streamer.finish('obsolete final')
    assert calls == ['obsolete'], 'A retry must check the revoked gate before touching the SDK'


@pytest.mark.asyncio
async def test_telegram_withdraws_streamed_draft_and_never_edits_it_after_revision():
    from nullion import telegram_app as telegram
    gate, calls = TurnPublicationGate(), []
    class Draft:
        async def edit_text(self, text, **kwargs):
            calls.append(('edit', text))
        async def delete(self):
            calls.append(('delete', None))
    class Message:
        text = 'α'
        async def reply_text(self, text, **kwargs):
            calls.append(('send', text))
            return Draft()
    streamer = telegram._TelegramTextDeltaStreamer(loop=asyncio.get_running_loop(), message=Message(), publication_gate=gate)
    assert await streamer._send_or_edit('draft')
    gate.revoke()
    assert not await streamer.finish('obsolete final')
    assert calls == [('send', 'draft'), ('delete', None)]


@pytest.mark.asyncio
async def test_telegram_revocation_during_attachment_upload_stops_bundle_and_withdraws_receipt(tmp_path):
    from nullion import telegram_app as telegram
    gate, calls = TurnPublicationGate(), []
    class Sent:
        async def delete(self):
            calls.append('withdraw')
    class Message:
        async def reply_document(self, **kwargs):
            calls.append('upload')
            gate.revoke()
            return Sent()
    proxy = telegram._PublicationGuardedTelegramMessage(Message(), gate)
    with pytest.raises(telegram._SupersededTelegramDelivery):
        await proxy.reply_document(document=b'pdf bytes')
    await proxy.discard()
    with pytest.raises(telegram._SupersededTelegramDelivery):
        await proxy.reply_document(document=b'next file')
    assert calls == ['upload', 'withdraw']


@pytest.mark.parametrize('case', ['independent', 'reset', 'other_parent', 'other_workspace', 'failed', 'write', 'scope_grant'])
def test_revision_evidence_never_inherits_unrelated_receipts_or_permissions(tmp_path, case):
    from nullion.chat_operator import _registry_with_inflight_revision_evidence
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    from nullion.connections import workspace_id_for_principal
    from nullion.conversation_runtime import ConversationTurn
    from datetime import datetime, timezone
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = ToolRegistry()
    name = 'request_tool_scope' if case == 'scope_grant' else 'fixture_read'
    spec = ToolSpec(name=name, description='fixture', risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.WRITE if case == 'write' else ToolSideEffectClass.READ,
        requires_approval=False, timeout_seconds=5, input_schema={'type': 'object'})
    registry.register(spec, lambda invocation: None)
    runtime.store.add_conversation_turn(ConversationTurn('parent', 'web:a', 'branch', None,
        Disposition.INDEPENDENT, 'α', 'completed', datetime.now(timezone.utc)))
    runtime.store.add_conversation_event({'event_type': 'conversation.chat_turn', 'conversation_id': 'web:a',
        'turn_id': 'other' if case == 'other_parent' else 'parent',
        'workspace_id': 'foreign' if case == 'other_workspace' else workspace_id_for_principal('web:a'),
        'tool_results': [{'tool_name': name, 'status': 'failed' if case == 'failed' else 'completed',
                          'output': {'url': 'https://example.test/', 'text': 'source'}}]})
    if case == 'reset':
        runtime.store.add_conversation_event({'event_type': 'conversation.session_reset', 'conversation_id': 'web:a'})
    decision = TurnDispatchDecision(TurnDispatchPolicy.WAIT_FOR_ACTIVE, ('parent',),
        Disposition.INDEPENDENT if case == 'independent' else Disposition.REVISE)
    assert _registry_with_inflight_revision_evidence(registry, runtime=runtime, conversation_id='web:a',
        principal_id='owner', decision=decision, base_registry=registry) is registry


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['text', 'stream'])
async def test_shared_delivery_revocation_is_not_an_upload_failure_or_retry(phase):
    from nullion.messaging_adapters import execute_platform_reply_delivery
    gate, calls = TurnPublicationGate(), []
    async def send(text, *args):
        calls.append(phase)
        gate.revoke()
        return True
    result = await execute_platform_reply_delivery('obsolete', channel='fixture', target_id='1',
        publication_gate=gate, send_text=send,
        finalize_stream=send if phase == 'stream' else None)
    assert calls == [phase]
    assert not result.text_sent and not result.fallback_text_sent
    assert result.receipt.error == 'turn_superseded'


@pytest.mark.asyncio
async def test_discord_draft_is_withdrawn_when_revised():
    from nullion.discord_app import _DiscordTextDeltaStreamer
    gate, calls = TurnPublicationGate(), []
    class Draft:
        async def edit(self, **kwargs):
            calls.append('edit')
        async def delete(self):
            calls.append('delete')
    class Channel:
        async def send(self, text):
            calls.append('send')
            return Draft()
    streamer = _DiscordTextDeltaStreamer(loop=asyncio.get_running_loop(), channel=Channel(), publication_gate=gate)
    assert await streamer._send_or_edit('draft')
    gate.revoke()
    assert not await streamer.finish('obsolete')
    assert calls == ['send', 'delete']


@pytest.mark.asyncio
async def test_shared_attachment_delivery_does_not_send_failure_fallback_after_revision(tmp_path, monkeypatch):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.messaging_adapters import DeliveryContract, execute_platform_reply_delivery
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    root = artifact_root_for_principal('owner')
    root.mkdir(parents=True, exist_ok=True)
    artifact = root / 'report.txt'
    artifact.write_bytes(b'actual report bytes')
    gate, uploads, text = TurnPublicationGate(), [], []
    async def send_text(value):
        text.append(value)
        return True
    async def upload(caption, paths):
        uploads.extend(path.read_bytes() for path in paths)
        gate.revoke()
        return True
    result = await execute_platform_reply_delivery('Report attached.', channel='fixture', target_id='1',
        principal_id='owner', delivery_contract=DeliveryContract.attachment_required(source='fixture'),
        artifact_paths=[str(artifact)], publication_gate=gate, send_text=send_text, send_attachments=upload)
    assert uploads == [b'actual report bytes']
    assert not text and not result.fallback_text_sent and not result.attachments_sent
    assert result.receipt.error == 'turn_superseded'

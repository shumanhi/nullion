"""Exercise real ingress, routing, tool evidence, persistence and transport edits."""
from tests.workflow_provider_protocol import workflow_provider_reply
import asyncio
import json
import os
import threading
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


def _runtime_fixture(tmp_path, monkeypatch, amendment):
    from nullion import chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "false")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "false")
    chat_store._store = None
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    from datetime import datetime, timezone
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id="fixture-network", principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector="departures.example.test",
        created_by="fixture-user", created_at=datetime.now(timezone.utc)))
    registry = ToolRegistry()
    entered, release = threading.Event(), threading.Event()
    calls, contexts, scopes = [], [], []
    source_marker = "source-receipt-73"

    def read(invocation):
        calls.append(invocation.tool_name)
        entered.set()
        assert release.wait(10)
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {
            "url": "https://departures.example.test/board", "text": source_marker,
            "departures": [{"id": "service-a", "time": "11:20", "platform": "4"}]})

    registry.register(ToolSpec(name="fixture_board_read", description="Read a current departure board",
        risk_level=ToolRiskLevel.LOW, side_effect_class=ToolSideEffectClass.READ,
        requires_approval=False, timeout_seconds=15, capability_tags=("web",),
        input_schema={"type": "object", "properties": {"url": {"type": "string"}}}), read)
    runtime.active_tool_registry = registry

    class Model:
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            context = json.dumps(kw.get("messages", []), ensure_ascii=False)
            if "allowed_target_indexes" in context:
                return {"content": [{"type": "text", "text": json.dumps({"relationship": "follow_up",
                    "effect": "revise", "confidence": 1, "target_index": 0, "target_is_explicit": True})}]}
            names = {tool["name"] for tool in kw.get("tools", [])}
            if not names:
                scopes.append(["model-decision", str(kw.get("system") or "")[:160]])
                if "semantic completion reviewer" in str(kw.get("system") or ""):
                    return {"content": [{"type": "text", "text": json.dumps({"disposition": "complete", "unresolved_requirements": [], "retry_tool_names": []})}]}
                return {"content": [{"type": "text", "text": "{}"}]}
            contexts.append(context)
            scopes.append(sorted(names))
            if amendment in context:
                assert "Inspect the current departure board." in context
                assert source_marker in context, "Revision lost the completed source receipt"
                text = "UPDATED_RESULT: second departure point, service-a at 11:20, platform 4."
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}
            if not calls:
                if "fixture_board_read" not in names:
                    return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "scope",
                        "name": "request_tool_scope", "input": {"capabilities": ["web"],
                        "tool_names": ["fixture_board_read"], "required_tool_names": ["fixture_board_read"]}}]}
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "read",
                    "name": "fixture_board_read", "input": {"url": "https://departures.example.test/board"}}]}
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "OBSOLETE_RESULT: departure point still missing."}]}

    model = Model()
    return SimpleNamespace(runtime=runtime, registry=registry, model=model,
        orchestrator=AgentOrchestrator(model_client=model), entered=entered, release=release, calls=calls, scopes=scopes)


@pytest.mark.parametrize("amendment", ["Use the second departure point.", "Usa el segundo punto de salida.", "出発地点を二つ目に変更してください。"])
def test_accepted_revision_never_publishes_obsolete_answer_and_reuses_source(tmp_path, monkeypatch, amendment):
    from nullion import telegram_app
    from nullion.config import load_settings
    from nullion.runtime_persistence import load_runtime_store
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker
    fixture = _runtime_fixture(tmp_path, monkeypatch, amendment)
    runtime, model = fixture.runtime, fixture.model
    entered, release, calls, scopes = fixture.entered, fixture.release, fixture.calls, fixture.scopes
    wire = []
    settings = load_settings(env={"NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings,
        bot_token=settings.telegram.bot_token, operator_chat_id="123", model_client=model,
        agent_orchestrator=fixture.orchestrator)
    service._turn_dispatch_tracker = AsyncTurnDispatchTracker()
    acknowledged = None

    class Sent:
        def __init__(self, owner, text):
            self.owner, self.text = owner, text
            self.message_id = 1000 + len(wire)
        async def edit_text(self, text, **kwargs):
            self.text = text
            wire.append((self.owner, "edit", text))
            return self
        async def delete(self):
            wire.append((self.owner, "delete", self.text))

    class Message:
        caption = None
        reply_to_message = None
        from_user = SimpleNamespace(id=123, first_name="Example", last_name="", username="example")
        def __init__(self, text, mid):
            self.text, self.message_id = text, mid
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
        async def send_action(self, action):
            pass
        async def reply_text(self, text, **kwargs):
            wire.append((self.message_id, "send", text))
            if self.message_id == 2:
                acknowledged.set()
            return Sent(self.message_id, text)

    async def run():
        nonlocal acknowledged
        acknowledged = asyncio.Event()
        async def deliver(text, mid):
            await service.on_message(SimpleNamespace(update_id=mid, message=Message(text, mid)), SimpleNamespace(bot=None))
        parent = asyncio.create_task(deliver("Inspect the current departure board.", 1))
        assert await asyncio.to_thread(entered.wait, 10), (wire, scopes,
            [(e.get("event_type"), e.get("assistant_reply"), e.get("tool_results"))
             for e in runtime.store.list_conversation_events("telegram:123")
             if e.get("event_type") == "conversation.chat_turn"])
        child = asyncio.create_task(deliver(amendment, 2))
        try:
            await asyncio.wait_for(acknowledged.wait(), 10)
        finally:
            release.set()
        await asyncio.wait_for(asyncio.gather(parent, child), 20)

    asyncio.run(run())
    assert not any("OBSOLETE_RESULT" in text for _, action, text in wire if action != "delete"), wire
    assert any("UPDATED_RESULT" in text for _, action, text in wire if action != "delete"), (wire, scopes,
        [e for e in runtime.store.list_conversation_events("telegram:123") if e.get("event_type") == "conversation.chat_turn"])
    assert calls == ["fixture_board_read"], "A revision must reuse completed evidence, not restart the lookup"
    stored = load_runtime_store(tmp_path / "runtime.db")
    child = stored.get_conversation_turn("telegram-update:2")
    assert child.parent_turn_id == "telegram-update:1"
    assert child.disposition.value == "revise"


@pytest.mark.parametrize("amendment", ["Use the second departure point.", "Usa el segundo punto de salida.", "出発地点を二つ目に変更してください。"])
@pytest.mark.parametrize('history_hydration', ['normal', 'delayed'])
def test_web_browser_amendment_replaces_running_answer_and_retains_evidence(tmp_path, monkeypatch, amendment, history_hydration):
    from playwright.sync_api import expect
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page

    fixture = _runtime_fixture(tmp_path, monkeypatch, amendment)
    app = create_app(fixture.runtime, fixture.orchestrator, fixture.registry)
    held_history = []
    def configure(page):
        if history_hydration == 'delayed':
            def hold_first(route):
                if not held_history:
                    held_history.append(route)
                else:
                    route.continue_()
            page.route('**/api/chat/history/**', hold_first)
    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url, configure_page=configure)
        try:
            if history_hydration == 'delayed':
                import time
                deadline = time.monotonic() + 5
                while not held_history and time.monotonic() < deadline:
                    page.wait_for_timeout(20)
                assert held_history
                held_history[0].continue_()
            # History hydration may insert the initial greeting after the input
            # becomes visible. Count the baseline only after the real restore
            # finishes; otherwise that greeting looks like a duplicated answer.
            page.wait_for_function('() => _chatSaveEnabled === true', timeout=10000)
            initial_bot_rows = page.locator("#messages .msg.bot").count()
            page.locator("#user-input").fill("Inspect the current departure board.")
            page.locator("#user-input").press("Enter")
            assert fixture.entered.wait(10), (fixture.scopes, [(e.get("tool_results")) for e in fixture.runtime.store.conversation_events if e.get("event_type") == "conversation.chat_turn"])
            page.locator("#user-input").fill(amendment)
            page.locator("#user-input").press("Enter")
            expect(page.locator("#messages")).to_contain_text(amendment)
            # Wait for actual typed admission before letting the parent finish.
            import time
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                events = fixture.runtime.store.conversation_events
                if any(e.get("event_type") == "conversation.reply_superseded" for e in events):
                    break
                page.wait_for_timeout(20)
            else:
                pytest.fail("The browser correction was not admitted as an amendment")
            fixture.release.set()
            expect(page.locator("#messages")).to_contain_text("UPDATED_RESULT", timeout=20000)
            expect(page.locator("#messages")).not_to_contain_text("OBSOLETE_RESULT")
            assert fixture.calls == ["fixture_board_read"]
            # The successor's final frame and the parent's superseded frame
            # may arrive in either order. Wait for their actual UI cleanup,
            # rather than sampling between these two valid terminal events.
            page.wait_for_function("() => _pendingTerminalReplyRecoveries.size === 0 && _botTurnBubbles.size === 0", timeout=5000)
            assert page.evaluate("_pendingTerminalReplyRecoveries.size") == 0
            assert page.evaluate("_botTurnBubbles.size") == 0
            assert page.locator("#messages .msg.bot").count() == initial_bot_rows + 1, page.locator('#messages .msg.bot').all_text_contents()
            page.screenshot(path=str(tmp_path / "amended-answer.png"))
            assert not errors
        finally:
            fixture.release.set()
            browser.close()
            playwright.stop()


@pytest.mark.parametrize("transport", ["http", "websocket"])
def test_web_real_route_amendment_discards_parent_and_links_successor(tmp_path, monkeypatch, transport):
    import concurrent.futures
    import time
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    amendment = "出発地点を二つ目に変更してください。"
    fixture = _runtime_fixture(tmp_path, monkeypatch, amendment)
    app = create_app(fixture.runtime, fixture.orchestrator, fixture.registry)
    parent = {"text": "Inspect the current departure board.", "conversation_id": "web:amend", "turn_id": "parent"}
    child = {"text": amendment, "conversation_id": "web:amend", "turn_id": "child"}
    with TestClient(app) as client, concurrent.futures.ThreadPoolExecutor() as executor:
        def release_when_revoked():
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if fixture.runtime.store.list_recent_conversation_events(
                        "web:amend", event_type="conversation.reply_superseded"):
                    fixture.release.set()
                    return
                time.sleep(.01)
            fixture.release.set()
            raise AssertionError("No typed revision admission")
        if transport == "http":
            origin = executor.submit(client.post, "/api/chat", json=parent)
            assert fixture.entered.wait(10)
            revised = executor.submit(client.post, "/api/chat", json=child)
            release_when_revoked()
            old, updated = origin.result(20).json(), revised.result(20).json()
            assert old["superseded"] is True and old["text"] == "" and old["artifacts"] == []
            assert "UPDATED_RESULT" in updated["text"], updated
        else:
            with client.websocket_connect("/ws/chat") as socket:
                socket.send_json(parent)
                assert fixture.entered.wait(10)
                socket.send_json(child)
                release_when_revoked()
                events, done = [], set()
                while len(done) < 2:
                    event = socket.receive_json()
                    events.append(event)
                    if event.get("type") == "done":
                        done.add(event["turn_id"])
                assert any(e.get("turn_id") == "parent" and e.get("superseded") for e in events)
                assert not any("OBSOLETE_RESULT" in str(e) for e in events)
                assert any("UPDATED_RESULT" in str(e) for e in events)
        assert fixture.calls == ["fixture_board_read"]
        assert fixture.runtime.store.get_conversation_turn("child").parent_turn_id == "parent"
        recovered = client.get('/api/chat/history/web:amend', params={"turn_ids": '["parent","child"]'}).json()
        assert [r["turn_id"] for r in recovered["terminal_replies"]] == ["child"]


def test_plain_http_turns_keep_fast_path_with_slow_optional_relationship_dependency(tmp_path, monkeypatch):
    import time
    import nullion.turn_dispatch_graph as dispatch
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    fixture = _runtime_fixture(tmp_path, monkeypatch, 'unused')
    def slow_relationship(*args, **kwargs):
        time.sleep(1)
        raise AssertionError('A plain non-overlapping chat entered relationship classification')
    monkeypatch.setattr(dispatch, '_model_turn_disposition', slow_relationship)
    class PlainModel:
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'PLAIN_RESULT: ready.'}]}
    from nullion.agent_orchestrator import AgentOrchestrator
    fixture.orchestrator = AgentOrchestrator(model_client=PlainModel())
    app = create_app(fixture.runtime, fixture.orchestrator, fixture.registry)
    elapsed = []
    with TestClient(app) as client:
        for prompt in ['Bonjour', 'Hola', 'こんにちは', 'Hello', 'Ciao']:
            start = time.monotonic()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': 'web:plain'})
            elapsed.append(time.monotonic() - start)
            assert 'PLAIN_RESULT' in response.json()['text'], response.json()
    assert sum(elapsed) / len(elapsed) < .75, elapsed
    assert sorted(elapsed)[-1] < 1.5, elapsed
    assert not fixture.calls


@pytest.mark.parametrize('platform', ['slack', 'discord'])
def test_platform_sdk_boundary_amendment_uses_real_runtime_and_one_source(tmp_path, monkeypatch, platform):
    from nullion import telegram_app, slack_app, discord_app
    from nullion.config import load_settings
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker
    amendment = 'Usa el segundo punto de salida.'
    fixture = _runtime_fixture(tmp_path, monkeypatch, amendment)
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '123', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true',
        'NULLION_SLACK_OPERATOR_USER_ID': 'U1', 'NULLION_DISCORD_OPERATOR_USER_ID': '123',
        'NULLION_SLACK_ENABLED': 'true', 'NULLION_DISCORD_ENABLED': 'true'})
    service = telegram_app.ChatOperatorService(runtime=fixture.runtime, settings=settings,
        bot_token=settings.telegram.bot_token, operator_chat_id='123', model_client=fixture.model,
        agent_orchestrator=fixture.orchestrator)
    adapter = slack_app if platform == 'slack' else discord_app
    monkeypatch.setattr(adapter, 'GLOBAL_TURN_DISPATCH_TRACKER', AsyncTurnDispatchTracker())
    sent = []
    class Draft:
        async def edit(self, **kwargs):
            sent.append(kwargs.get('content', ''))
        async def delete(self):
            pass
    class Channel:
        id = 'channel-1'
        async def send(self, text):
            sent.append(text)
            return Draft()
    channel = Channel()
    async def say(text):
        sent.append(text)
        return {'ok': True, 'ts': 'output-1'}
    async def deliver(text, ident):
        if platform == 'slack':
            await slack_app.handle_slack_message(service, settings,
                event={'user': 'U1', 'text': text, 'channel': 'C1', 'event_ts': str(ident),
                       'client_msg_id': f'client-{ident}'}, say=say)
        else:
            await discord_app.handle_discord_message(service, settings,
                SimpleNamespace(author=SimpleNamespace(id=123, bot=False), content=text, id=str(ident),
                                channel=channel, attachments=[]))
    async def run():
        origin = asyncio.create_task(deliver('Inspect the current departure board.', 1))
        assert await asyncio.to_thread(fixture.entered.wait, 10), (sent, origin.exception() if origin.done() else 'running', fixture.scopes)
        revised = asyncio.create_task(deliver(amendment, 2))
        try:
            async def admitted():
                while not any(e.get('event_type') == 'conversation.reply_superseded'
                              for e in fixture.runtime.store.conversation_events):
                    await asyncio.sleep(.01)
            await asyncio.wait_for(admitted(), 10)
        finally:
            fixture.release.set()
        await asyncio.wait_for(asyncio.gather(origin, revised), 20)
    asyncio.run(run())
    assert any('UPDATED_RESULT' in text for text in sent), sent
    assert not any('OBSOLETE_RESULT' in text for text in sent), sent
    assert fixture.calls == ['fixture_board_read']
    child = fixture.runtime.store.get_conversation_turn('client-2' if platform == 'slack' else '2')
    assert child and child.parent_turn_id == ('client-1' if platform == 'slack' else '1')


def test_independent_http_request_finishes_while_another_source_lookup_is_running(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    fixture = _runtime_fixture(tmp_path, monkeypatch, 'unused')
    class ParallelModel:
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            context = json.dumps(kwargs.get('messages', []))
            if 'allowed_target_indexes' in context:
                return {'content': [{'type': 'text', 'text': json.dumps({
                    'relationship': 'separate', 'effect': 'continue', 'confidence': 1,
                    'target_index': None, 'target_is_explicit': False})}]}
            if 'SECOND_REQUEST' in context:
                return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'SECOND_RESULT'}]}
            return fixture.model.create(**kwargs)
    app = create_app(fixture.runtime, AgentOrchestrator(model_client=ParallelModel()), fixture.registry)
    with TestClient(app) as client, ThreadPoolExecutor() as pool:
        parent = pool.submit(client.post, '/api/chat', json={
            'text': 'Inspect the current departure board.', 'conversation_id': 'web:parallel', 'turn_id': 'first'})
        assert fixture.entered.wait(10)
        try:
            second = pool.submit(client.post, '/api/chat', json={
                'text': 'SECOND_REQUEST: respond with the requested marker.',
                'conversation_id': 'web:parallel', 'turn_id': 'second'}).result(5).json()
            assert 'SECOND_RESULT' in second['text']
            assert not parent.done(), 'Independent requests were serialized behind active work'
        finally:
            fixture.release.set()
        assert parent.result(10).status_code == 200
        child = fixture.runtime.store.get_conversation_turn('second')
        assert child and child.disposition.value == 'independent' and child.parent_turn_id is None

"""Real release Git history, SQLite claims and native messaging SDK bytes."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import sqlite3
import threading
import time
from contextlib import suppress

import pytest

from nullion import update_notifications as notices
from tests.test_update_notifications import release

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def test_running_telegram_loop_discovers_new_release_without_chat(release, monkeypatch):
    from tests.test_updater_release_channel import _git
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
    from nullion.telegram_app import send_telegram_update_notice
    from nullion.telegram_transport import build_telegram_bot
    from telegram import Bot
    _git(release.repo, 'tag', '-d', 'v0.2.0')
    _git(release.repo, 'push', 'origin', ':refs/tags/v0.2.0')
    monkeypatch.setattr(notices, 'CHECK_INTERVAL', 0.1)
    wire = TelegramWire()
    install_wire(monkeypatch, wire)
    async def replay():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            task = asyncio.create_task(notices.run_update_notification_loop(release.runtime,
                channel='telegram', target=lambda: '77',
                send=lambda target, text: send_telegram_update_notice(bot, target, text),
                initial_delay=0, poll_interval=0.05))
            try:
                deadline = time.monotonic() + 10
                while not release.runtime.checkpoint_path.exists() and time.monotonic() < deadline:
                    await asyncio.sleep(0.02)
                # A running instance sees no update before publication.
                while time.monotonic() < deadline:
                    with sqlite3.connect(release.runtime.checkpoint_path) as db:
                        ready = db.execute("SELECT name FROM sqlite_master WHERE name='update_release_checks'").fetchone()
                        row = db.execute('SELECT result FROM update_release_checks').fetchone() if ready else None
                    if row and json.loads(row[0])['latest'] == 'v0.1.0':
                        break
                    await asyncio.sleep(0.02)
                else:
                    pytest.fail('Initial release check never completed')
                assert wire.texts == []
                await asyncio.to_thread(_git, release.repo, 'tag', 'v0.2.0', release.new)
                await asyncio.to_thread(_git, release.repo, 'push', 'origin', '--tags')
                while not wire.texts and time.monotonic() < deadline:
                    await asyncio.sleep(0.02)
                assert len(wire.texts) == 1 and 'v0.2.0' in wire.texts[0]
                await asyncio.sleep(0.35)
                assert len(wire.texts) == 1
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
    asyncio.run(replay())
    with sqlite3.connect(release.runtime.checkpoint_path) as db:
        assert db.execute('SELECT status FROM update_release_notices').fetchall() == [('sent',)]


@contextmanager
def platform_wire(*, discord_lost_ack=False):
    accepted = []
    nonces = set()
    user = {'id': '123', 'username': 'Operator', 'discriminator': '0000', 'avatar': None}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def reply(self, value, status=200):
            raw = json.dumps(value).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        def do_GET(self):
            self.reply(dict(user, id='999', bot=True) if self.path.endswith('/@me') else user)
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            if self.path.endswith('/chat.postMessage'):
                accepted.append(body['text'])
                self.reply({'ok': True, 'channel': body['channel'], 'ts': '1.00001'})
            elif self.path.endswith('/users/@me/channels'):
                self.reply({'id': '777', 'type': 1, 'recipients': [user]})
            elif self.path.endswith('/channels/777/messages'):
                assert body['enforce_nonce'] is True and body['nonce']
                if body['nonce'] not in nonces:
                    nonces.add(body['nonce'])
                    accepted.append(body['content'])
                    if discord_lost_ack:
                        self.reply({'message': 'Accepted before gateway error', 'code': 0}, status=500)
                        return
                self.reply({'id': str(1000 + len(accepted)), 'channel_id': '777', 'author': user,
                    'content': body['content'], 'timestamp': '2026-11-15T00:00:00+00:00',
                    'edited_timestamp': None, 'tts': False, 'mention_everyone': False,
                    'mentions': [], 'mention_roles': [], 'attachments': [], 'embeds': [], 'type': 0})
            else:
                self.send_error(404)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/', accepted
    finally:
        server.shutdown(); server.server_close(); thread.join(5)


@pytest.mark.parametrize('surface', ['telegram', 'slack', 'discord'])
def test_native_notice_is_delivered_once_across_restart(release, monkeypatch, surface):
    from nullion.runtime import bootstrap_persistent_runtime
    runtime = bootstrap_persistent_runtime(release.runtime.checkpoint_path)
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        monkeypatch.delenv(key, raising=False)
    async def exercise(send, target):
        assert await notices.deliver_update_notice(runtime, channel=surface, target=target, send=send, now=100)
        runtime.checkpoint(force=True)
        restored = bootstrap_persistent_runtime(runtime.checkpoint_path)
        assert not await notices.deliver_update_notice(restored, channel=surface, target=target, send=send, now=101)
        assert not any(await asyncio.gather(*(notices.deliver_update_notice(restored,
            channel=surface, target=target, send=send, now=4000) for _ in range(4))))
    if surface == 'telegram':
        from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
        from nullion.telegram_app import send_telegram_update_notice
        from nullion.telegram_transport import build_telegram_bot
        from telegram import Bot
        wire = TelegramWire(); install_wire(monkeypatch, wire)
        async def replay():
            async with build_telegram_bot(Bot, TOKEN) as bot:
                await exercise(lambda target, text: send_telegram_update_notice(bot, target, text), '77')
        asyncio.run(replay())
        accepted = wire.texts
    else:
        with platform_wire(discord_lost_ack=surface == 'discord') as (base_url, accepted):
            async def replay():
                if surface == 'slack':
                    from slack_sdk.web.async_client import AsyncWebClient
                    from nullion.slack_app import send_slack_update_notice
                    client = AsyncWebClient(token='synthetic', base_url=base_url, retry_handlers=[])
                    await exercise(lambda target, text: send_slack_update_notice(client, target, text), 'U1')
                else:
                    import discord
                    from nullion.discord_app import send_discord_update_notice
                    monkeypatch.setattr(discord.http.Route, 'BASE', base_url.rstrip('/'))
                    async with discord.Client(intents=discord.Intents.none()) as client:
                        data = await client.http.static_login('synthetic')
                        client._connection.user = discord.ClientUser(state=client._connection, data=data)
                        await exercise(lambda target, text: send_discord_update_notice(client, target, text), 'user:123')
            asyncio.run(replay())
    assert len(accepted) == 1 and 'Nullion v0.2.0 is available.' in accepted[0]
    assert ('/nullion update' if surface == 'slack' else '/update') in accepted[0]
    with sqlite3.connect(runtime.checkpoint_path) as db:
        assert db.execute('SELECT status FROM update_release_notices').fetchall() == [('sent',)]
    assert not runtime.store.list_conversation_events(event_types=('conversation.model_timing',))


def test_telegram_accepted_notice_with_lost_ack_is_never_repeated(release, monkeypatch):
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
    from nullion.telegram_app import send_telegram_update_notice
    from nullion.telegram_transport import build_telegram_bot
    from telegram import Bot
    wire = TelegramWire(mode='unknown_notice'); install_wire(monkeypatch, wire)
    async def replay():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            for now in (100, 101, 4000):
                assert not await notices.deliver_update_notice(release.runtime, channel='telegram', target='77',
                    send=lambda target, text: send_telegram_update_notice(bot, target, text), now=now)
    asyncio.run(replay())
    assert len(wire.texts) == 1


def test_web_update_button_tracks_releases_and_clears_after_install(release, tmp_path, monkeypatch):
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from tests.test_updater_release_channel import _git
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry
    from playwright.sync_api import expect
    runtime = isolate(tmp_path, monkeypatch)
    with _UvicornThread(create_app(runtime, orchestrator=None, registry=ToolRegistry()), _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.evaluate('checkForUpdates()')
            button = page.locator('#update-btn')
            expect(button).to_be_visible()
            expect(button).to_have_text('Update (v0.2.0)')
            _git(release.repo, 'checkout', '--detach', release.new)
            page.evaluate('checkForUpdates()')
            expect(button).to_be_hidden()
            assert not page.request.get(server.base_url + '/api/version').json()['has_update']
        finally:
            browser.close()
            playwright.stop()


def test_slow_background_release_fetch_does_not_block_plain_chat(release, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import updater
    from nullion.web_app import create_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    entered, finish = threading.Event(), threading.Event()
    original = updater._update_target
    def slow(*args, **kwargs):
        entered.set()
        assert finish.wait(10)
        return original(*args, **kwargs)
    monkeypatch.setattr(updater, '_update_target', slow)
    class Provider:
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            return protocol if protocol is not None else text('こんにちは！')
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=Provider()), ToolRegistry())) as client:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(client.get, '/api/version')
            assert entered.wait(5)
            try:
                times = []
                for index in range(3):
                    start = time.perf_counter()
                    reply = client.post('/api/chat', json={'text': 'こんにちは', 'conversation_id': f'web:hello-{index}'})
                    times.append(time.perf_counter() - start)
                    assert reply.status_code == 200 and 'こんにちは' in reply.json()['text'], reply.text
                assert sum(times) / len(times) < 1 and max(times) < 2, times
                assert not pending.done(), 'Slow release check should still be running independently'
                print({'plain_chat_seconds': times})
            finally:
                finish.set()
            assert pending.result(timeout=5).json()['latest'] == 'v0.2.0'

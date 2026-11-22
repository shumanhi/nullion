import asyncio
import json
from types import SimpleNamespace

import pytest

from nullion.plugins.browser_plugin.browser_tab_lease import BrowserTabLease, owned_browser_type


def test_shared_constructor_does_not_grant_permissions_or_emulate_user_viewports():
    from nullion.plugins.browser_plugin.browser_use_adapter import _browser_context_parameters
    options = _browser_context_parameters(shared=True)
    assert options['permissions'] == []
    assert options['viewport'] is None and options['no_viewport'] is True
    assert options['headless'] is False


def test_dedicated_local_constructor_retains_its_existing_profile_policy():
    from nullion.plugins.browser_plugin.browser_use_adapter import _browser_context_parameters
    options = _browser_context_parameters(shared=False)
    assert options['viewport'] == {'width': 1440, 'height': 1000}
    assert 'permissions' not in options and 'no_viewport' not in options


@pytest.mark.asyncio
async def test_shared_session_disables_cosmetic_blank_tab_fanout(tmp_path):
    mutations = []
    class Base:
        async def attach_all_watchdogs(self):
            async def decorate(): mutations.append('decorate')
            async def replace(): mutations.append('replace')
            self._aboutblank_watchdog = SimpleNamespace(_stopping=False,
                _show_dvd_screensaver_on_about_blank_tabs=decorate,
                _check_and_ensure_about_blank_tab=replace)
    browser = owned_browser_type(Base, BrowserTabLease('http://browser.test', tmp_path/'lease.json'))()
    await browser.attach_all_watchdogs()
    watchdog = browser._aboutblank_watchdog
    for _ in range(20):
        await watchdog._show_dvd_screensaver_on_about_blank_tabs()
        await watchdog._check_and_ensure_about_blank_tab()
    assert watchdog._stopping is True
    assert mutations == []


def test_lease_records_exact_ids_atomically_in_private_file(tmp_path):
    lease = BrowserTabLease("http://browser.test", tmp_path/"lease.json")
    async def send(**kw): return {"targetId": "独立37"}
    browser = SimpleNamespace(cdp_client=SimpleNamespace(send_raw=send))
    lease.install(browser)
    asyncio.run(browser.cdp_client.send_raw("Target.createTarget", {"url": "about:blank"}))
    asyncio.run(browser.cdp_client.send_raw("Target.createTarget", {"url": "about:blank"}))
    assert json.loads(lease.path.read_text())["target_ids"] == ["独立37"]
    assert lease.path.stat().st_mode & 0o777 == 0o600
    assert not lease.path.with_suffix(".next").exists()


@pytest.mark.asyncio
async def test_startup_guard_preserves_unowned_newtab_and_allows_owned_navigation(tmp_path):
    lease = BrowserTabLease("ws://browser.test", tmp_path / "lease.json")
    calls = []
    async def send(**kwargs):
        calls.append(kwargs)
        return {}
    browser = SimpleNamespace(cdp_client=SimpleNamespace(send_raw=send),
        session_manager=SimpleNamespace(get_target_id_from_session_id=lambda session: session))
    lease.install(browser)
    with pytest.raises(PermissionError):
        await browser.cdp_client.send_raw("Page.navigate", {"url": "about:blank"}, "usuario")
    assert calls == []
    lease.record("自分")
    await browser.cdp_client.send_raw("Page.navigate", {"url": "about:blank"}, "自分")
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('ready', [False, True])
async def test_protocol_close_requires_exact_owner_or_verified_owned_child(tmp_path, ready):
    lease = BrowserTabLease('ws://browser.test', tmp_path/'lease.json')
    lease.record('owned')
    lease.ready = ready
    calls = []
    async def send(**kwargs):
        calls.append(kwargs)
        return {'success': True}
    async def info(params):
        return {'targetInfo': {'targetId': params['targetId'], 'type': 'page',
            'openerId': 'owned' if params['targetId'] == 'child' else None}}
    browser = SimpleNamespace(cdp_client=SimpleNamespace(send_raw=send,
        send=SimpleNamespace(Target=SimpleNamespace(getTargetInfo=info))))
    lease.install(browser)
    with pytest.raises(PermissionError):
        await browser.cdp_client.send_raw('Target.closeTarget', {'targetId': 'user'})
    assert not calls
    for target in ('owned', 'child'):
        await browser.cdp_client.send_raw('Target.closeTarget', {'targetId': target})
    assert [row['params']['targetId'] for row in calls] == ['owned', 'child']
    assert lease.owns('child')


@pytest.mark.asyncio
async def test_root_replacement_tracks_startup_and_reconnected_targets_once(tmp_path):
    lease = BrowserTabLease("ws://browser.test", tmp_path / "lease.json")
    class Base:
        @property
        def cdp_client(self):
            return self._cdp_client_root
    browser = owned_browser_type(Base, lease)()
    for target in ("inicio", "再接続"):
        async def send(**kwargs):
            return {"targetId": target}
        browser._cdp_client_root = SimpleNamespace(send_raw=send)
        tracked = browser.cdp_client.send_raw
        lease.install(browser)
        assert browser.cdp_client.send_raw is tracked
        await browser.cdp_client.send_raw("Target.createTarget", {"url": "about:blank"})
    assert lease.target_ids == {"inicio", "再接続"}


@pytest.mark.asyncio
async def test_cleanup_closes_only_owned_ids_and_owned_popups_and_retries_ack(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin.backends import cdp_backend
    targets = [{"targetId": "own", "type": "page"}, {"targetId": "popup", "type": "page", "openerId": "own"},
        {"targetId": "user-new", "type": "page"}, {"targetId": "user-old", "type": "page"}]
    attempts = []
    class Client:
        def __init__(self, *args): pass
        async def send(self, method, params=None, **kwargs):
            if method == "Target.getTargets": return {"targetInfos": targets[:]}
            target = params["targetId"]; attempts.append(target)
            if len(attempts) <= 2: raise ConnectionError("Temporary acknowledgement failure")
            targets[:] = [t for t in targets if t["targetId"] != target]
            return {"success": True}
        async def close(self): pass
    monkeypatch.setattr(cdp_backend, "_RawCDPClient", Client)
    lease = BrowserTabLease("ws://browser.test", tmp_path/"lease.json"); lease.record("own")
    result = await lease.cleanup()
    assert result["verified"] and result["closed_count"] == 2
    assert {t["targetId"] for t in targets} == {"user-old", "user-new"}
    assert set(attempts) == {"own", "popup"} and not lease.path.exists()


@pytest.mark.asyncio
async def test_unacknowledged_cleanup_retains_ownership_for_parent(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin.backends import cdp_backend
    class Client:
        def __init__(self, *args): pass
        async def send(self, *args, **kwargs): raise ConnectionError("Connection lost")
        async def close(self): pass
    monkeypatch.setattr(cdp_backend, "_RawCDPClient", Client)
    lease = BrowserTabLease("ws://browser.test", tmp_path/"lease.json"); lease.record("own")
    result = await lease.cleanup()
    assert not result["verified"] and result["remaining_target_ids"] == ["own"]
    assert json.loads(lease.path.read_text())["target_ids"] == ["own"]


@pytest.mark.asyncio
async def test_worker_navigation_creates_owned_tab_instead_of_reusing_user_blank(tmp_path):
    lease = BrowserTabLease("ws://browser.test", tmp_path/"lease.json")
    class Base:
        agent_focus_target_id = "user-blank"
        async def _cdp_create_new_page(self, url): lease.record("own"); return "own"
        async def get_or_create_cdp_session(self, *, target_id, focus):
            self.agent_focus_target_id = target_id
            return SimpleNamespace(target_id=target_id)
        async def on_NavigateToUrlEvent(self, event): return self.agent_focus_target_id
        async def on_SwitchTabEvent(self, event): return event.target_id
        async def on_CloseTabEvent(self, event): return event.target_id
    lease.ready = True
    browser = owned_browser_type(Base, lease)()
    async def unknown(browser, target_id): return lease.owns(target_id)
    lease.recognize = unknown
    assert await browser.on_NavigateToUrlEvent(SimpleNamespace(new_tab=True)) == "own"
    for name in ("on_SwitchTabEvent", "on_CloseTabEvent"):
        with pytest.raises(PermissionError):
            await getattr(browser, name)(SimpleNamespace(target_id="user-blank"))


@pytest.mark.asyncio
async def test_recovered_user_focus_cannot_be_read_or_used_for_actions(tmp_path):
    lease = BrowserTabLease("ws://browser.test", tmp_path / "lease.json")
    lease.ready = True
    reads = []
    sessions = []
    class Base:
        agent_focus_target_id = "usuario"
        async def get_current_target_info(self):
            reads.append(self.agent_focus_target_id)
            return {"targetId": self.agent_focus_target_id}
        async def get_or_create_cdp_session(self, target_id=None, focus=True):
            sessions.append(target_id)
            self.agent_focus_target_id = target_id or "usuario"
            return SimpleNamespace(target_id=self.agent_focus_target_id)
    async def recognize(browser, target_id):
        return lease.owns(target_id)
    lease.recognize = recognize
    browser = owned_browser_type(Base, lease)()
    with pytest.raises(PermissionError):
        await browser.get_current_target_info()
    assert not reads and browser.agent_focus_target_id is None
    browser.agent_focus_target_id = "usuario"
    with pytest.raises(PermissionError):
        await browser.get_or_create_cdp_session()
    assert browser.agent_focus_target_id is None and not sessions


@pytest.mark.asyncio
async def test_missing_focus_recovers_surviving_owned_tab_before_user_tab(tmp_path):
    lease = BrowserTabLease("ws://browser.test", tmp_path/'lease.json')
    lease.record('自分')
    lease.ready = True
    class Base:
        agent_focus_target_id = None
        session_manager = SimpleNamespace(get_all_page_targets=lambda: [
            SimpleNamespace(target_id='自分'), SimpleNamespace(target_id='usuario')])
        async def get_or_create_cdp_session(self, target_id=None, focus=True):
            self.agent_focus_target_id = target_id or 'usuario'
            return SimpleNamespace(target_id=self.agent_focus_target_id)
        async def get_current_target_info(self):
            return {'targetId': self.agent_focus_target_id}
    browser = owned_browser_type(Base, lease)()
    assert (await browser.get_current_target_info())['targetId'] == '自分'
    assert browser.agent_focus_target_id == '自分'


def test_browser_task_failure_retains_numbered_recovery_choices(monkeypatch):
    from nullion.plugins.browser_plugin.browser_use_adapter import browser_task_handler
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.backends.session_recovery import BrowserRecoveryRequired
    from nullion.tools import ToolInvocation
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda: True)
    def disconnected(invocation):
        raise BrowserRecoveryRequired()
    handler = browser_task_handler(lambda: object(), BrowserPolicy(), connection_getter=disconnected)
    result = handler(ToolInvocation('recuperar', 'browser_run_task', 'operator',
        {'task': '確認', 'url': 'https://example.com'}))
    assert result.status == 'failed'
    assert result.output['result_kind'] == 'user_choice_required'
    assert result.output['saved_profile_preserved'] is True
    assert result.output['reconnect_attempted'] is True
    assert [option['number'] for option in result.output['recovery_options']] == [1, 2]


def test_source_compaction_retains_cleanup_and_bounded_performance():
    from nullion.source_observation_contract import compact_source_observations
    output = {"result_kind": "source_observations", "observations": [], "sources": [],
        "browser_tab_cleanup": {"created_target_count": 2, "closed_count": 2, "verified": True, "remaining_target_ids": []},
        "performance": {"model_calls": 15, "model_ms": 190000, "untrusted_optional_data": "x"*10000}}
    compact = compact_source_observations(output, max_chars=2000)
    assert compact["browser_tab_cleanup"]["verified"] is True
    assert compact["performance"] == {"model_calls": 15, "model_ms": 190000}
    output['browser_tab_cleanup'].update(verified=False, remaining_target_ids=['pending-37'])
    for _ in range(3):
        output = compact_source_observations(output, max_chars=2000)
        assert output['browser_tab_cleanup']['remaining_target_count'] == 1
        assert output['browser_tab_cleanup']['verified'] is False

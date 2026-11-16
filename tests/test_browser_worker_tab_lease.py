import asyncio
import json
from types import SimpleNamespace

import pytest

from nullion.plugins.browser_plugin.browser_tab_lease import BrowserTabLease, owned_browser_type


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
        async def get_or_create_cdp_session(self, *, target_id, focus): self.agent_focus_target_id = target_id
        async def on_NavigateToUrlEvent(self, event): return self.agent_focus_target_id
        async def on_SwitchTabEvent(self, event): return event.target_id
        async def on_CloseTabEvent(self, event): return event.target_id
    lease.ready = True
    browser = owned_browser_type(Base, lease)()
    async def unknown(*args): return False
    lease.recognize = unknown
    assert await browser.on_NavigateToUrlEvent(SimpleNamespace(new_tab=True)) == "own"
    for name in ("on_SwitchTabEvent", "on_CloseTabEvent"):
        with pytest.raises(PermissionError):
            await getattr(browser, name)(SimpleNamespace(target_id="user-blank"))


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

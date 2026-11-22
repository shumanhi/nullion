"""Exact target ownership for a worker attached to a user's CDP browser."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4


class BrowserTabLease:
    def __init__(self, endpoint: str, path: Path | str | None = None):
        from nullion.plugins.browser_plugin.browser_config import nullion_runtime_home
        self.endpoint = endpoint
        self.path = Path(path) if path else nullion_runtime_home() / "browser-tab-leases" / (uuid4().hex + ".json")
        self.target_ids: set[str] = set()
        self.closed_ids: set[str] = set()
        self.ready = False

    def record(self, target_id: str):
        if target_id in self.target_ids:
            return
        self.target_ids.add(target_id)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".next")
        temporary.write_text(json.dumps({"endpoint": self.endpoint, "target_ids": sorted(self.target_ids)}))
        temporary.chmod(0o600)
        os.replace(temporary, self.path)

    def owns(self, target_id):
        return target_id in self.target_ids

    def install(self, browser, *, client=None):
        """Record SDK-created targets at the protocol boundary, not by URL."""
        client = client if client is not None else browser.cdp_client
        send = client.send_raw
        if getattr(send, "_nullion_tab_lease", None) is self:
            return
        async def tracked(method, params=None, session_id=None):
            if method == "Target.closeTarget" and not await self.recognize(browser, (params or {}).get("targetId")):
                raise PermissionError("This tab belongs to another browser user or task")
            # Browser Use 0.13.10 normalizes existing chrome://newtab pages
            # inside connect(), independently of AboutBlankWatchdog. Refuse
            # that startup navigation before it changes an unowned user tab.
            if not self.ready and method == "Page.navigate" and (params or {}).get("url") == "about:blank":
                manager = getattr(browser, "session_manager", None)
                target = manager.get_target_id_from_session_id(session_id) if manager else None
                if not self.owns(target):
                    raise PermissionError("Startup cannot navigate another browser user's tab")
            result = await send(method=method, params=params, session_id=session_id)
            if method == "Target.createTarget" and isinstance(result.get("targetId"), str):
                self.record(result["targetId"])
            return result
        tracked._nullion_tab_lease = self
        client.send_raw = tracked

    async def recognize(self, browser, target_id):
        if self.owns(target_id):
            return True
        if not target_id:
            return False
        info = (await browser.cdp_client.send.Target.getTargetInfo({"targetId": target_id}))["targetInfo"]
        if info.get("type") in {"page", "tab"} and self.owns(info.get("openerId")):
            self.record(target_id)
            return True
        return False

    async def cleanup(self):
        """Detach first; independently close/verify only recorded IDs and popups."""
        from nullion.plugins.browser_plugin.backends.cdp_backend import _RawCDPClient, _read_json_url
        client = None
        remaining = set(self.target_ids)
        try:
            endpoint = self.endpoint
            if endpoint.startswith(("http://", "https://")):
                version = await asyncio.to_thread(_read_json_url, endpoint.rstrip("/") + "/json/version")
                endpoint = version["webSocketDebuggerUrl"]
            client = _RawCDPClient(endpoint)
            targets = (await client.send("Target.getTargets", timeout=3))["targetInfos"]
            # A popup opened by an owned page belongs to the same task. A new
            # user tab has no owned opener and must survive concurrent cleanup.
            changed = True
            while changed:
                changed = False
                for target in targets:
                    if (target.get("type") in {"page", "tab"} and self.owns(target.get("openerId"))
                            and not self.owns(target["targetId"])):
                        self.record(target["targetId"])
                        changed = True
            present = {t["targetId"] for t in targets}
            remaining = self.target_ids & present
            for _ in range(2):
                await asyncio.gather(*(client.send("Target.closeTarget", {"targetId": target}, timeout=2)
                    for target in remaining), return_exceptions=True)
                deadline = asyncio.get_running_loop().time() + 1
                while True:
                    present = {t["targetId"] for t in (await client.send("Target.getTargets", timeout=2))["targetInfos"]}
                    remaining = self.target_ids & present
                    if not remaining or asyncio.get_running_loop().time() >= deadline:
                        break
                    await asyncio.sleep(.05)
                if not remaining:
                    break
            self.closed_ids.update(self.target_ids - remaining)
            if not remaining:
                self.path.unlink(missing_ok=True)
        except Exception:
            # Retain the exact ledger for the parent after worker termination.
            pass
        finally:
            if client is not None:
                await client.close()
        return {"created_target_count": len(self.target_ids), "closed_count": len(self.closed_ids),
            "remaining_target_ids": sorted(remaining), "verified": not remaining}


async def cleanup_worker_tab_lease(path):
    path = Path(path)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    lease = BrowserTabLease(data["endpoint"], path)
    lease.target_ids = set(data["target_ids"])
    return await asyncio.wait_for(lease.cleanup(), timeout=8)


def owned_browser_type(base, lease):
    """Keep SDK navigation away from existing tabs, including blank user tabs."""
    class OwnedSecurityEvents:
        def __init__(self, browser, watchdog):
            self.browser, self.watchdog = browser, watchdog

        async def on_TabCreatedEvent(self, event):
            if await lease.recognize(self.browser, event.target_id):
                return await self.watchdog.on_TabCreatedEvent(event)

        async def on_NavigationCompleteEvent(self, event):
            if await lease.recognize(self.browser, event.target_id):
                return await self.watchdog.on_NavigationCompleteEvent(event)

    class OwnedBrowser(base):
        def __setattr__(self, name, value):
            super().__setattr__(name, value)
            # The pinned SDK creates/replaces its root client in connect() and
            # reconnect(), before a public connected event. Keep this private
            # integration here so ownership also covers startup and recovery.
            if name == "_cdp_client_root" and value is not None:
                lease.install(self, client=value)

        async def get_current_target_info(self):
            # SDK focus recovery can select an arbitrary existing page after
            # the task tab closes, bypassing SwitchTabEvent. Never expose that
            # page to the agent's observation/actor APIs.
            if lease.ready and self.agent_focus_target_id is None:
                await self.get_or_create_cdp_session()
            if lease.ready and not await lease.recognize(self, self.agent_focus_target_id):
                self.agent_focus_target_id = None
                raise PermissionError("The task's browser tab is no longer available")
            result = await super().get_current_target_info()
            if lease.ready and result and not await lease.recognize(self, result["targetId"]):
                self.agent_focus_target_id = None
                raise PermissionError("This tab belongs to another browser user or task")
            return result

        async def get_or_create_cdp_session(self, target_id=None, focus=True):
            if lease.ready and target_id is None:
                candidate = self.agent_focus_target_id
                if await lease.recognize(self, candidate):
                    target_id = candidate
                else:
                    self.agent_focus_target_id = None
                    manager = getattr(self, "session_manager", None)
                    targets = manager.get_all_page_targets() if manager else []
                    owned = [target for target in targets if lease.owns(target.target_id)]
                    if not owned:
                        raise PermissionError("The task's browser tab is no longer available")
                    target_id = owned[-1].target_id
            if lease.ready and target_id is not None and focus and not await lease.recognize(self, target_id):
                raise PermissionError("This tab belongs to another browser user or task")
            session = await super().get_or_create_cdp_session(target_id=target_id, focus=focus)
            if lease.ready and (target_id is None or focus) and not await lease.recognize(self, session.target_id):
                self.agent_focus_target_id = None
                raise PermissionError("The task's browser tab is no longer available")
            return session

        async def attach_all_watchdogs(self):
            await super().attach_all_watchdogs()
            # These SDK handlers operate beyond the task's targets: download
            # setup changes the default context and popup setup accepts user
            # dialogs. Detach through the pinned SDK's supported handler hook.
            watchers = [getattr(self, name, None) for name in ("_downloads_watchdog", "_popups_watchdog")]
            security = getattr(self, "_security_watchdog", None)
            if any(watchers) or security is not None:
                from browser_use.browser.events import TabCreatedEvent, NavigationCompleteEvent
            for watcher in watchers:
                if watcher is not None:
                    watcher.detach_handler_from_session(self, TabCreatedEvent, watcher.on_TabCreatedEvent)
            if security is not None and not getattr(security, "_nullion_owned_events", None):
                callbacks = OwnedSecurityEvents(self, security)
                for event_type in (TabCreatedEvent, NavigationCompleteEvent):
                    handler_name = "on_" + event_type.__name__
                    security.detach_handler_from_session(self, event_type, getattr(security, handler_name))
                    security.attach_handler_to_session(self, event_type, getattr(callbacks, handler_name))
                object.__setattr__(security, "_nullion_owned_events", callbacks)
            # The SDK's cosmetic blank-page watchdog scans and decorates every
            # blank tab for each TabCreatedEvent. On a shared session this both
            # modifies unowned tabs and creates quadratic startup event traffic.
            # Keep its lifecycle handlers, but disable shared-browser decoration
            # and automatic replacement tabs. Task tabs belong to the lease.
            watchdog = self._aboutblank_watchdog
            watchdog._stopping = True
            async def leave_shared_blank_tabs_alone():
                return None
            object.__setattr__(watchdog, '_show_dvd_screensaver_on_about_blank_tabs',
                leave_shared_blank_tabs_alone)
            object.__setattr__(watchdog, '_check_and_ensure_about_blank_tab',
                leave_shared_blank_tabs_alone)

        async def on_NavigateToUrlEvent(self, event):
            if lease.ready and (event.new_tab or not await lease.recognize(self, self.agent_focus_target_id)):
                target = await self._cdp_create_new_page("about:blank")
                await self.get_or_create_cdp_session(target_id=target, focus=True)
                event.new_tab = False
            return await super().on_NavigateToUrlEvent(event)

        async def on_SwitchTabEvent(self, event):
            if not lease.ready:
                return await super().on_SwitchTabEvent(event)
            if event.target_id is None:
                owned = [t for t in self.session_manager.get_all_page_targets() if lease.owns(t.target_id)]
                event.target_id = owned[-1].target_id if owned else await self._cdp_create_new_page("about:blank")
            if not await lease.recognize(self, event.target_id):
                raise PermissionError("This tab belongs to another browser user or task")
            return await super().on_SwitchTabEvent(event)

        async def on_CloseTabEvent(self, event):
            if lease.ready and not await lease.recognize(self, event.target_id):
                raise PermissionError("This tab belongs to another browser user or task")
            return await super().on_CloseTabEvent(event)
    return OwnedBrowser

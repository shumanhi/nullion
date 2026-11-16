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

    def install(self, browser):
        """Record SDK-created targets at the protocol boundary, not by URL."""
        client = browser.cdp_client
        send = client.send_raw
        async def tracked(method, params=None, session_id=None):
            result = await send(method=method, params=params, session_id=session_id)
            if method == "Target.createTarget" and isinstance(result.get("targetId"), str):
                self.record(result["targetId"])
            return result
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
    class OwnedBrowser(base):
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

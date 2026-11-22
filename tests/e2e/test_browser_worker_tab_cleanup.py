"""Real isolated Browser Use/CDP and request cancellation, preserving user tabs."""
import json
import os
from pathlib import Path
import site
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


@pytest.mark.parametrize("scenario", ["reconnect", "missing_focus", "focus_recovery", "cancel_startup", "timeout_startup", "failed_startup", "agent_init_failure"])
def test_real_sdk_attachment_preserves_newtab_and_releases_owned_targets(scenario, tmp_path):
    """Use the pinned SDK and a disposable container Chromium, with no model."""
    worker = os.environ.get("NULLION_BROWSER_USE_TEST_PYTHON")
    assert worker, "This regression requires the isolated Browser Use SDK"
    script = r'''
import asyncio, json, socket, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
sys.path.extend(json.loads(sys.argv[2]))
import browser_use
from playwright.async_api import async_playwright
from nullion.plugins.browser_plugin.browser_tab_lease import BrowserTabLease, owned_browser_type
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask, run_browser_task
from types import SimpleNamespace

async def main():
    root=Path(sys.argv[3]); scenario=sys.argv[4]
    sock=socket.socket(); sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]; sock.close()
    endpoint=f'http://127.0.0.1:{port}'
    async with async_playwright() as p:
        context=await p.chromium.launch_persistent_context(str(root/'profile'), channel='chromium',
            headless=True, args=[f'--remote-debugging-port={port}', '--no-sandbox'])
        try:
            original=context.pages[0]
            await original.goto('chrome://newtab/')
            if scenario == 'focus_recovery':
                await original.goto('data:text/html,<title>Personal</title><button onclick="window.clicked=true">Private action</button>')
            original_url=original.url
            if scenario in {'reconnect', 'missing_focus', 'focus_recovery'}:
                lease=BrowserTabLease(endpoint,root/'lease.json')
                browser=owned_browser_type(browser_use.Browser,lease)(cdp_url=endpoint,keep_alive=True,
                    user_data_dir=None,enable_default_extensions=False,accept_downloads=False,auto_download_pdfs=False)
                try:
                    await browser.start()
                    first=await browser.new_page()
                    await browser.get_or_create_cdp_session(target_id=first._target_id,focus=True)
                    lease.ready=True
                    if scenario == 'missing_focus':
                        browser.agent_focus_target_id=None
                        restored=await browser.get_current_page()
                        assert restored._target_id == first._target_id
                        assert browser.agent_focus_target_id == first._target_id
                    elif scenario == 'focus_recovery':
                        await browser.cdp_client.send.Target.closeTarget({'targetId':first._target_id})
                        for _ in range(100):
                            if browser.agent_focus_target_id != first._target_id and not browser.session_manager._recovery_in_progress:
                                break
                            await asyncio.sleep(.05)
                        # The SDK's fallback is a real unowned user page. Both
                        # the actor/read API and action-session API must refuse it.
                        assert browser.agent_focus_target_id not in lease.target_ids
                        for operation in (browser.get_current_page,
                                lambda: browser.get_or_create_cdp_session(target_id=browser.agent_focus_target_id)):
                            try:
                                await operation()
                            except (PermissionError, ValueError):
                                pass
                            else:
                                raise AssertionError('User tab exposed after focus recovery')
                        assert not await original.evaluate('Boolean(window.clicked)')
                    else:
                        old_client=browser.cdp_client
                        # Exercise the SDK's real root replacement without the old
                        # socket's drop callback racing this deliberate reconnect.
                        browser._intentional_stop=True
                        await browser.reconnect()
                        browser._intentional_stop=False
                        second=await browser.new_page()
                        assert browser.cdp_client is not old_client
                        assert {first._target_id,second._target_id} <= lease.target_ids
                finally:
                    await browser.stop()
                    cleanup=await lease.cleanup()
                assert cleanup['verified'] and cleanup['created_target_count'] == (2 if scenario == 'reconnect' else 1), cleanup
            else:
                started=asyncio.Event(); attached=[]
                real_start=browser_use.Browser.start
                async def controlled_start(self):
                    await real_start(self)
                    attached.append(self)
                    if scenario in {'cancel_startup', 'timeout_startup', 'failed_startup'}:
                        await self.new_page()
                        started.set()
                        if scenario == 'failed_startup':
                            raise RuntimeError('接続失敗')
                        await asyncio.Event().wait()
                browser_use.Browser.start=controlled_start
                if scenario == 'agent_init_failure':
                    class UnavailableAgent:
                        def __init__(self, **kwargs):
                            raise RuntimeError('initialisation indisponible')
                    browser_use.Agent=UnavailableAgent
                policy=BrowserPolicy(); policy.block_private=False
                pending=asyncio.create_task(run_browser_task(
                    BrowserTask(task='確認',url='https://example.com',max_steps=2),
                    client=SimpleNamespace(model='unused'),principal_id='review',policy=policy,
                    timeout=3 if scenario == 'timeout_startup' else 20,
                    browser_connection=(endpoint,True),tab_lease_path=root/'lease.json'))
                if scenario == 'cancel_startup':
                    await asyncio.wait_for(started.wait(),10)
                    pending.cancel()
                    try:
                        await asyncio.wait_for(pending,15)
                    except asyncio.CancelledError:
                        pass
                    else:
                        raise AssertionError('Cancellation must propagate')
                elif scenario in {'timeout_startup', 'failed_startup'}:
                    result=await asyncio.wait_for(pending,20)
                    assert result['browser_session_recovery']['kind'] == 'same_session_startup_handoff', result
                else:
                    try:
                        await asyncio.wait_for(pending,20)
                    except RuntimeError as error:
                        assert 'initialisation indisponible' in str(error), error
                    else:
                        raise AssertionError('Agent initialization must fail')
                assert attached and all(b._cdp_client_root is None for b in attached)
                assert not (root/'lease.json').exists()
            assert original.url == original_url, (original_url, original.url)
            if scenario != 'focus_recovery':
                assert original_url in {'chrome://newtab/', 'chrome://new-tab-page/'}
            assert len(context.pages) == 1, [page.url for page in context.pages]
            assert not original.is_closed()
        finally:
            await context.close()
asyncio.run(main())
'''
    env = os.environ.copy()
    env["ANONYMIZED_TELEMETRY"] = "false"
    env["NULLION_HOME"] = str(tmp_path / "home")
    result = subprocess.run([worker, "-c", script,
        str(Path(__file__).resolve().parents[2] / "src"), json.dumps(site.getsitepackages()),
        str(tmp_path), scenario], env=env, capture_output=True, text=True, timeout=50)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('headless_env', ['true', 'false', None])
def test_shared_profile_preserves_seeded_permissions_and_user_viewports(tmp_path, headless_env):
    """Exercise the real adapter constructor, pinned SDK and native CDP state."""
    worker = os.environ.get("NULLION_BROWSER_USE_TEST_PYTHON")
    assert worker, "This regression requires the isolated Browser Use SDK"
    script = r'''
import asyncio, json, socket, sys, threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
sys.path.insert(0, sys.argv[1]); sys.path.extend(json.loads(sys.argv[2]))
import browser_use
from playwright.async_api import async_playwright
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask, run_browser_task, _browser_context_parameters
from cdp_use import CDPClient

class Source(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b'<title>Personal tab</title><main>Untouched user document</main>')
    def log_message(self, *args): pass

async def main():
    root=Path(sys.argv[3]); server=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    url=f'http://127.0.0.1:{server.server_port}/'
    sock=socket.socket(); sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]; sock.close()
    endpoint=f'http://127.0.0.1:{port}'
    async with async_playwright() as p:
        context=await p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',
            headless=True,args=[f'--remote-debugging-port={port}','--no-sandbox'])
        try:
            pages=[context.pages[0],await context.new_page()]
            for index,page in enumerate(pages):
                await page.goto((url if index==0 else url.replace('127.0.0.1','localhost'))+str(index))
                await page.set_viewport_size({'width':913+index*134,'height':677+index*56})
            # Playwright owns the launch context and applies its exact native
            # context identity. Granting this list denies other permissions at
            # the seeded origin; the second origin retains prompt defaults.
            await context.grant_permissions(['geolocation'],origin=url.rstrip('/'))
            await asyncio.sleep(.15)
            for page in pages:
                await page.evaluate("() => { window.resizeCount=0; addEventListener('resize',()=>window.resizeCount++); }")
            async def state(page):
                return await page.evaluate("""async () => ({url:location.href,title:document.title,
                    width:innerWidth,height:innerHeight,screenWidth:screen.width,screenHeight:screen.height,
                    scale:devicePixelRatio,resizes:window.resizeCount,
                    permissions:await Promise.all(['notifications','geolocation','clipboard-read'].map(
                        async name => (await navigator.permissions.query({name})).state))})""")
            baseline=[await state(page) for page in pages]
            assert baseline[0]['permissions']==['denied','granted','denied'],baseline
            assert baseline[1]['permissions']==['prompt','prompt','prompt'],baseline
            real_start=browser_use.Browser.start
            phases=[]; attached=[]; commands=[]
            real_send=CDPClient.send_raw
            async def observe_send(self,method,params=None,session_id=None):
                commands.append((method,params,session_id))
                return await real_send(self,method=method,params=params,session_id=session_id)
            CDPClient.send_raw=observe_send
            async def assert_unchanged(phase):
                await asyncio.sleep(.15)
                current=[await state(page) for page in pages]
                assert current==baseline,(phase,baseline,current)
                phases.append(phase)
            async def controlled_start(self):
                attached.append(self)
                assert self.browser_profile.permissions==[]
                assert self.browser_profile.no_viewport is True and self.browser_profile.viewport is None
                assert self.browser_profile.headless is False
                await real_start(self)
                assert self.browser_profile.no_viewport is True and self.browser_profile.viewport is None
                await assert_unchanged('attached')
                old=self.cdp_client; self._intentional_stop=True
                await self.reconnect(); self._intentional_stop=False
                assert self.cdp_client is not old
                await assert_unchanged('reconnected')
                await self.new_page()
                raise RuntimeError('Completed native shared-state probe')
            browser_use.Browser.start=controlled_start
            # Validate the dedicated local constructor against the same pinned
            # SDK without launching another browser or altering its policy.
            local=browser_use.Browser(user_data_dir=None,**_browser_context_parameters(shared=False))
            assert local.browser_profile.viewport.width==1440 and local.browser_profile.viewport.height==1000
            assert local.browser_profile.permissions==browser_use.browser.profile.BrowserProfile().permissions
            policy=BrowserPolicy(); policy.block_private=False
            result=await run_browser_task(BrowserTask(task='確認',url=url,max_steps=2),
                client=SimpleNamespace(model='unused'),principal_id='review',policy=policy,timeout=20,
                browser_connection=(endpoint,True),tab_lease_path=root/'lease.json')
            assert result['browser_session_recovery']['kind']=='same_session_startup_handoff',result
            assert attached and all(b._cdp_client_root is None for b in attached)
            await assert_unchanged('cleaned')
            assert phases==['attached','reconnected','cleaned'],phases
            assert len(context.pages)==2 and not (root/'lease.json').exists()
            forbidden={'Browser.grantPermissions','Browser.setPermission','Browser.resetPermissions',
                'Browser.setDownloadBehavior','Emulation.setDeviceMetricsOverride','Emulation.clearDeviceMetricsOverride'}
            assert not [row for row in commands if row[0] in forbidden],commands
        finally:
            await context.close(); server.shutdown(); server.server_close()
asyncio.run(main())
'''
    env = os.environ.copy()
    env["ANONYMIZED_TELEMETRY"] = "false"
    env["NULLION_HOME"] = str(tmp_path / "home")
    env.pop('DISPLAY', None)
    if headless_env is None:
        env.pop('NULLION_BROWSER_HEADLESS', None)
    else:
        env['NULLION_BROWSER_HEADLESS'] = headless_env
    result = subprocess.run([worker, "-c", script,
        str(Path(__file__).resolve().parents[2] / "src"), json.dumps(site.getsitepackages()), str(tmp_path)],
        env=env, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr


def test_shared_startup_preserves_downloads_dialogs_and_unowned_policy_tabs(tmp_path):
    worker = os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    assert worker
    script = r'''
import asyncio,json,socket,sys,threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
sys.path.insert(0,sys.argv[1]);sys.path.extend(json.loads(sys.argv[2]))
import browser_use
from cdp_use import CDPClient
from playwright.async_api import async_playwright
from browser_use.browser.events import TabCreatedEvent,NavigateToUrlEvent
from nullion.plugins.browser_plugin.browser_tab_lease import BrowserTabLease,owned_browser_type
from nullion.plugins.browser_plugin.browser_use_adapter import _browser_context_parameters,_cleanup_browser_attachment
class Source(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        if self.path.startswith('/download-'):
            self.send_header('Content-Type','application/octet-stream')
            self.send_header('Content-Disposition','attachment; filename="'+self.path[1:]+'.txt"')
            body=b'Native user-selected bytes'
        else:
            self.send_header('Content-Type','text/html');body=b'<title>Personal policy tab</title><main>Private user page</main>'
        self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass
async def main():
    root=Path(sys.argv[3]);destination=root/'user-downloads';destination.mkdir()
    source=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=source.serve_forever,daemon=True).start()
    url=f'http://127.0.0.1:{source.server_port}/'
    forbidden=url.replace('127.0.0.1','localhost')+'personal'
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
    endpoint=f'http://127.0.0.1:{port}'
    async with async_playwright() as p:
        context=await p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',
            headless=True,args=[f'--remote-debugging-port={port}','--no-sandbox'])
        dialogs=[];browser=None
        try:
            user=context.pages[0];await user.goto(url)
            private=await context.new_page();await private.goto(forbidden)
            seed=await context.browser.new_browser_cdp_session()
            user_ids={t['targetId'] for t in (await seed.send('Target.getTargets'))['targetInfos'] if t['type']=='page'}
            await seed.send('Browser.setDownloadBehavior',{'behavior':'allow','downloadPath':str(destination),'eventsEnabled':True})
            async def download(phase):
                print('download '+phase,flush=True)
                name='download-'+phase
                await asyncio.wait_for(user.evaluate("name => { const a=document.createElement('a');a.href='/'+name;a.download=name+'.txt';a.click(); }",name),3)
                path=destination/(name+'.txt')
                for _ in range(100):
                    if path.is_file() and path.read_bytes()==b'Native user-selected bytes':return
                    await asyncio.sleep(.025)
                raise AssertionError('User download behavior changed during '+phase)
            await download('before')
            commands=[];real_send=CDPClient.send_raw
            async def observed(self,method,params=None,session_id=None):
                commands.append((method,params,session_id))
                return await real_send(self,method=method,params=params,session_id=session_id)
            CDPClient.send_raw=observed
            lease=BrowserTabLease(endpoint,root/'lease.json')
            browser=owned_browser_type(browser_use.Browser,lease)(cdp_url=endpoint,keep_alive=True,
                user_data_dir=None,enable_default_extensions=False,accept_downloads=False,auto_download_pdfs=False,
                downloads_path=str(root/'task-downloads'),allowed_domains=['127.0.0.1'],**_browser_context_parameters(shared=True))
            await asyncio.wait_for(browser.start(),15);await asyncio.sleep(.2)
            assert not private.is_closed() and private.url==forbidden
            assert not [e for e in browser.event_bus.event_history.values() if e.event_type=='BrowserErrorEvent']
            await download('attached')
            own=await browser.new_page()
            await browser.get_or_create_cdp_session(target_id=own._target_id,focus=True)
            lease.ready=True
            try:await browser._security_watchdog.on_NavigateToUrlEvent(NavigateToUrlEvent(url=forbidden))
            except ValueError:pass
            else:raise AssertionError('Task navigation policy was bypassed')
            own_session=await browser.get_or_create_cdp_session(target_id=own._target_id,focus=True)
            await own_session.cdp_client.send.Page.navigate({'url':forbidden},session_id=own_session.session_id)
            for _ in range(100):
                info=(await browser.cdp_client.send.Target.getTargetInfo({'targetId':own._target_id}))['targetInfo']
                if info['url']==forbidden:break
                await asyncio.sleep(.01)
            assert info['url']==forbidden
            event=browser.event_bus.dispatch(TabCreatedEvent(target_id=own._target_id,url=info['url']))
            await event;await event.event_result(raise_if_any=True,raise_if_none=False)
            for _ in range(100):
                present={t['targetId'] for t in (await seed.send('Target.getTargets'))['targetInfos']}
                if own._target_id not in present:break
                await asyncio.sleep(.01)
            assert own._target_id not in present and not private.is_closed()
            print('owned policy checked',flush=True)
            task=await browser.new_page()
            await browser.get_or_create_cdp_session(target_id=task._target_id,focus=True)
            private.on('dialog',lambda dialog:dialogs.append(dialog))
            await private.evaluate("() => {setTimeout(()=>{window.userConfirm=confirm('Private confirmation must remain pending')},0)}")
            for _ in range(100):
                if dialogs:break
                await asyncio.sleep(.01)
            assert len(dialogs)==1
            browser._intentional_stop=True
            await asyncio.wait_for(browser.reconnect(),10)
            browser._intentional_stop=False
            assert not private.is_closed() and not browser._closed_popup_messages,browser._closed_popup_messages
            await download('reconnected')
            print('cleanup starting',flush=True)
            await asyncio.wait_for(_cleanup_browser_attachment(browser,lease),15)
            assert browser._cdp_client_root is None
            await download('cleaned')
            # A real user dialog remains pending through reconnect and cleanup.
            # Only the fixture's user dismisses it now, preserving Cancel.
            assert len(dialogs)==1;await dialogs[0].dismiss()
            assert await private.evaluate('window.userConfirm') is False
            actual={t['targetId'] for t in (await seed.send('Target.getTargets'))['targetInfos'] if t['type']=='page'}
            assert actual==user_ids,(actual,user_ids)
            assert not (root/'lease.json').exists()
            assert not [row for row in commands if row[0] in {'Browser.grantPermissions','Browser.setPermission',
                'Browser.resetPermissions','Browser.setDownloadBehavior','Page.handleJavaScriptDialog'}],commands
            assert not [row for row in commands if row[0]=='Target.closeTarget' and row[1]['targetId'] in user_ids],commands
            await seed.detach()
        finally:
            for dialog in dialogs:
                try:await asyncio.wait_for(dialog.dismiss(),1)
                except Exception:pass
            await asyncio.wait_for(context.close(),5);source.shutdown();source.server_close()
asyncio.run(main())
'''
    env=os.environ.copy();env['ANONYMIZED_TELEMETRY']='false';env['NULLION_HOME']=str(tmp_path/'home')
    try:
        result=subprocess.run([worker,'-c',script,str(Path(__file__).resolve().parents[2]/'src'),
            json.dumps(site.getsitepackages()),str(tmp_path)],env=env,capture_output=True,text=True,timeout=50)
    except subprocess.TimeoutExpired as error:
        pytest.fail('Native shared-state probe timed out: '+str(error.stdout)+str(error.stderr))
    assert result.returncode==0,result.stdout+result.stderr


@pytest.mark.parametrize("outcome", ["completed", "cancelled"])
def test_browser_worker_closes_created_tabs_and_preserves_concurrent_user_tab(tmp_path, monkeypatch, outcome, record_property):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.browser_model_protocol import browser_schema_messages
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.source_answer_fixture import native_png_directives, source_presentation_response
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({"outbound_request_mode": "allow_all"})
    worker = os.environ.get("NULLION_BROWSER_USE_TEST_PYTHON")
    assert worker, "This regression requires the isolated Browser Use SDK"
    port = _free_port()
    endpoint = f"http://127.0.0.1:{port}"
    for key, value in {"NULLION_CODE_MODE": "on", "NULLION_BROWSER_BACKEND": "cdp",
            "NULLION_BROWSER_CDP_URL": endpoint, "NULLION_BROWSER_USE_CDP_URL": endpoint,
            "NULLION_BROWSER_USE_PYTHON": worker, "NULLION_BROWSER_BLOCK_PRIVATE": "false"}.items():
        monkeypatch.setenv(key, value)
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            # Simulate a messaging/cron configuration refresh after the native
            # backend has resolved and attached to its actual managed session.
            monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://127.0.0.1:1')
            self.send_response(200); self.end_headers()
            self.wfile.write(b"<html><title>Owned source</title><main>Verified value: 37</main></html>")
        def log_message(self, *args): pass
    source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f"http://localhost:{source.server_port}/one"
    reached = threading.Event(); release = threading.Event(); user_targets = []
    class Model:
        model = "scripted-external-provider"
        phase = 0
        browser_calls = 0
        def create(self, **kw):
            schema_messages = browser_schema_messages(kw)
            if schema_messages:
                self.browser_calls += 1
                schema = json.loads(schema_messages[-1]["content"].split(": ", 1)[1])
                if self.browser_calls == 1:
                    # A user-created tab during the job must not be swept up
                    # by a before/after target inventory difference.
                    request = urllib.request.Request(endpoint+"/json/new?about:blank", method="PUT")
                    user_targets.append(json.load(urllib.request.urlopen(request))["id"])
                    reached.set()
                    if outcome == "cancelled":
                        release.wait(25)
                    actions = {"navigate": {"url": url.replace("/one", "/two"), "new_tab": True}}
                else:
                    report = {"goal_status": "answered", "next_action": "report", "observations": [
                        {"label": "値", "value": "37", "quote": "Verified value: 37"}], "unknowns": []}
                    actions = {"done": {"data": report, "success": True}}
                value = {"evaluation_previous_goal": "Captured source", "memory": "37", "next_goal": "Report", "action": [actions]}
                for key in ("thinking", "current_plan_item", "plan_update"):
                    if key in schema.get("properties", {}): value[key] = "Inspect" if key == "thinking" else None
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "browser-"+str(self.browser_calls),
                    "name": "browser_decision", "input": value}]}
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            names = {t["name"] for t in kw.get("tools", [])}
            if "present_source_answer" in names: return source_presentation_response(kw)
            if not names: return text("{}")
            self.phase += 1
            if self.phase == 1:
                return call("request_tool_scope", {"capabilities": ["web"], "tool_names": ["browser_run_task", "browser_read_pages"],
                    "required_tool_names": ["browser_run_task"], "artifact_extensions": [".png"],
                    "source_artifact_delivery_requested": True, "source_artifact_request_evidence": url+" .png"})
            if self.phase == 2:
                return call("code_mode", {"tool_names": ["browser_read_pages", "browser_run_task"], "code":
                    "call_tool('browser_read_pages', "+repr({"pages": [{"url": url, "read_kind": "text"}]})+")\n"+
                    "call_tool('browser_run_task', "+repr({"task": "Capture the requested displayed value", "url": url, "max_steps": 4})+")"})
            return text("Verified value: 37. "+native_png_directives(kw, latest_only=True))
    model = Model(); registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    started = time.monotonic()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch_persistent_context(str(tmp_path/"profile"), headless=True,
                args=[f"--remote-debugging-port={port}"])
            try:
                original = browser.pages[0]
                original.goto("data:text/html,<title>User original</title>Untouched")
                blank = browser.new_page()
                # Below the all-target capacity guard: the SDK formerly scanned
                # all blank tabs for every startup event, flooding its queue.
                blank_tabs = [blank, *[browser.new_page() for _ in range(11)]]
                baseline = {t["id"] for t in json.load(urllib.request.urlopen(endpoint+"/json/list")) if t["type"] == "page"}
                with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pending = pool.submit(client.post, "/api/chat", json={"text": "選択した値をPNGで確認してください。 "+url+" .png",
                            "conversation_id": "web:worker-cleanup", "turn_id": "worker-cleanup"})
                        if not reached.wait(35):
                            client.post("/api/chat", json={"text": "/stop", "conversation_id": "web:worker-cleanup"})
                            release.set()
                            turns = runtime.store.list_recent_conversation_events("web:worker-cleanup", event_type="conversation.chat_turn")
                            pytest.fail("The isolated worker did not reach its model: "+repr([e.get('tool_results') for e in turns]))
                        if outcome == "cancelled":
                            stopped = client.post("/api/chat", json={"text": "/stop", "conversation_id": "web:worker-cleanup"})
                            assert stopped.status_code == 200
                        response = pending.result(timeout=45)
                    assert response.status_code == 200, response.text
                    if outcome == "completed":
                        assert "37" in response.json()["text"] and response.json().get("artifacts"), response.json()
                        turns = runtime.store.list_recent_conversation_events("web:worker-cleanup", event_type="conversation.chat_turn")
                        results = [r for r in turns[-1]["tool_results"] if r["tool_name"] == "browser_run_task"]
                        assert results[-1]["output"]["browser_tab_cleanup"]["verified"] is True, results
                    else:
                        assert response.json().get("superseded") is True, response.json()
                    deadline = time.monotonic()+12
                    expected = baseline | set(user_targets)
                    while True:
                        actual = {t["id"] for t in json.load(urllib.request.urlopen(endpoint+"/json/list")) if t["type"] == "page"}
                        leases = list((tmp_path/"NULLION_HOME"/"browser-tab-leases").glob("*.json"))
                        # Closing targets precedes the independent verification
                        # and ledger removal. Wait for both cleanup receipts;
                        # a cancelled HTTP observer can finish before its worker.
                        if (actual == expected and not leases) or time.monotonic() >= deadline: break
                        time.sleep(.1)
                    assert actual == expected, (actual, expected)
                    assert original.title() == "User original" and blank.url == "about:blank"
                    assert all(page.title() == '' and page.url == 'about:blank'
                        and page.locator('#pretty-loading-animation').count() == 0 for page in blank_tabs)
                    assert not leases, [json.loads(path.read_text()) for path in leases]
            finally:
                release.set()
                browser.close()
    finally:
        source.shutdown(); source.server_close()
    record_property(outcome+"_seconds", time.monotonic()-started)
    assert time.monotonic()-started < 45

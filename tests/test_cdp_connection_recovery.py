"""Failed borrowed-browser connections cannot wedge later browser operations."""
import asyncio
from types import SimpleNamespace
import pytest

@pytest.mark.parametrize('cancelled',[False,True])
def test_failed_connection_cleanup_has_a_deadline_and_clears_owned_transport(monkeypatch,cancelled):
    from nullion.plugins.browser_plugin.backends import cdp_backend as module
    stopped=[];fallback=[]
    class Driver:
        async def stop(self):
            try:await asyncio.sleep(60)
            finally:stopped.append(True)
    driver=Driver()
    async def connect(url):
        if cancelled:raise asyncio.CancelledError
        raise TimeoutError('External CDP connection stalled')
    driver.chromium=SimpleNamespace(connect_over_cdp=connect)
    class Manager:
        async def __aenter__(self):return driver
    monkeypatch.setattr(module,'async_playwright',Manager)
    async def targets(url):return []
    monkeypatch.setattr(module,'_async_cdp_targets',targets)
    backend=module.CDPBackend()
    async def raw():fallback.append(True);return 'read-only raw transport'
    monkeypatch.setattr(backend,'_ensure_raw_browser',raw)
    async def run():
        start=asyncio.get_running_loop().time()
        if cancelled:
            with pytest.raises(asyncio.CancelledError):await backend._ensure_browser()
        else:assert await backend._ensure_browser()=='read-only raw transport'
        assert asyncio.get_running_loop().time()-start<2.5
    asyncio.run(run())
    assert stopped==[True] and backend._playwright is None
    assert fallback==([] if cancelled else [True])

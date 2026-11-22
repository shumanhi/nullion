"""Failed borrowed-browser connections cannot wedge later browser operations."""
import asyncio
from types import SimpleNamespace
import pytest


@pytest.mark.parametrize('scheme,port', [('ws', 80), ('http', 80), ('wss', 443), ('https', 443)])
@pytest.mark.parametrize('host', ['configured.test', 'localhost', '[::1]'])
def test_explicit_endpoint_uses_its_protocol_default_port(monkeypatch, scheme, port, host):
    from nullion.plugins.browser_plugin.backends.auto_backend import _cdp_endpoint
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', f'{scheme}://{host}/devtools/browser/id')
    expected_host = 'configured.test' if host == 'configured.test' else '127.0.0.1'
    assert _cdp_endpoint() == (expected_host, port)


def test_unconfigured_and_bare_local_cdp_keep_the_documented_port(monkeypatch):
    from nullion.plugins.browser_plugin.backends.auto_backend import _cdp_endpoint
    from nullion.plugins.browser_plugin.backends.cdp_backend import cdp_endpoint_port
    monkeypatch.delenv('NULLION_BROWSER_CDP_URL', raising=False)
    monkeypatch.delenv('NULLION_BROWSER_USE_CDP_URL', raising=False)
    assert _cdp_endpoint() == ('127.0.0.1', 9222)
    assert cdp_endpoint_port('localhost') == 9222
    assert cdp_endpoint_port('wss://configured.test:19222/opaque') == 19222


@pytest.mark.asyncio
@pytest.mark.parametrize('scheme,port', [('ws', 80), ('http', 80), ('wss', 443), ('https', 443)])
async def test_auto_attach_and_captured_connection_probe_use_protocol_port(monkeypatch, scheme, port):
    from nullion.plugins.browser_plugin.backends import auto_backend as module
    endpoint = f'{scheme}://configured.test/opaque'
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', endpoint)
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'false')
    monkeypatch.delenv('NULLION_BROWSER_PREFERRED', raising=False)
    calls = []
    def reachable(host, number):
        calls.append((host, number))
        return number == port
    monkeypatch.setattr(module, '_is_cdp_reachable_at', reachable)
    monkeypatch.setattr(module, '_find_chrome', lambda: pytest.fail('Configured endpoint is already available'))
    class Connected:
        BACKEND_NAME = 'cdp'
        _cdp_url = endpoint
        async def extract_text(self, session_id): raise TimeoutError('まだ準備中')
    backend = module.AutoBackend()
    monkeypatch.setattr(backend, '_make_cdp', Connected)
    assert await backend.browser_task_connection('確認') == (endpoint, True)
    # A later settings reload must not alter the captured endpoint or its port.
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://unrelated.test:9222')
    with pytest.raises(TimeoutError): await backend._call('extract_text', '確認')
    assert calls == [('configured.test', port), ('configured.test', port)]


@pytest.mark.asyncio
async def test_auto_headless_resolves_explicit_local_task_connection(monkeypatch):
    from nullion.plugins.browser_plugin.backends.auto_backend import AutoBackend
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://unrelated.test:9222')
    backend = SessionRecoveryBackend(AutoBackend())
    assert await backend.browser_task_connection('確認') == (None, False)
    assert backend._configured._backend.BACKEND_NAME == 'playwright'


def test_resolved_local_connection_reaches_worker_without_environment_rediscovery(monkeypatch):
    from nullion.plugins.browser_plugin.backends.auto_backend import AutoBackend
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter, browser_use_worker as worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_tools import _run
    from nullion.tools import ToolInvocation
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://unrelated.test:9222')
    backend = SessionRecoveryBackend(AutoBackend())
    forwarded = []
    def run(task, **kwargs):
        forwarded.append(kwargs['browser_connection'])
        return {'artifact_paths': []}
    monkeypatch.setattr(adapter, 'browser_use_python', lambda: '/isolated/python')
    monkeypatch.setattr(worker, 'run_isolated_browser_task', run)
    handler = adapter.browser_task_handler(lambda: object(), BrowserPolicy(),
        mode_getter=lambda inv: backend.mode_for_session('確認'),
        connection_getter=lambda inv: _run(backend.browser_task_connection('確認')))
    result = handler(ToolInvocation('connection', 'browser_run_task', 'user',
        {'task': '確認', 'url': 'https://example.test'}))
    assert result.status == 'completed', result.error
    assert forwarded == [(None, False)]


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['configured', 'headless'])
async def test_selected_local_backend_never_rediscovers_a_cdp_connection(mode):
    from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    backend = SessionRecoveryBackend(PlaywrightBackend())
    if mode == 'headless':
        await backend.select_mode('確認', mode)
    assert await backend.browser_task_connection('確認') == (None, False)


@pytest.mark.asyncio
@pytest.mark.parametrize('reconnect_succeeds', [True, False])
async def test_task_connection_resolution_uses_bounded_recovery(reconnect_succeeds):
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend, BrowserRecoveryRequired
    class Configured:
        BACKEND_NAME = 'auto'
        attempts = reconnects = 0
        async def browser_task_connection(self, session):
            self.attempts += 1
            if self.attempts == 1: raise ConnectionError('オフライン')
            return ('http://configured.test:9222', True)
        async def reconnect(self, session):
            self.reconnects += 1
            if not reconnect_succeeds: raise ConnectionError('indisponible')
    configured = Configured()
    backend = SessionRecoveryBackend(configured)
    if reconnect_succeeds:
        assert await backend.browser_task_connection('確認') == ('http://configured.test:9222', True)
    else:
        with pytest.raises(BrowserRecoveryRequired): await backend.browser_task_connection('確認')
    assert configured.reconnects == 1
    assert configured.attempts == (2 if reconnect_succeeds else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize('scheme', ['ws', 'wss'])
@pytest.mark.parametrize('connects', [True, False])
async def test_websocket_attach_bypasses_http_discovery(monkeypatch, scheme, connects):
    from nullion.plugins.browser_plugin.backends import cdp_backend as module
    endpoint = scheme + '://configured.test/devtools/browser/opaque'
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', endpoint)
    calls = []
    browser = SimpleNamespace(is_connected=lambda: True)
    async def connect(url):
        calls.append(url)
        if not connects: raise TimeoutError('external websocket unavailable')
        return browser
    async def stop(): calls.append('stop')
    class Manager:
        async def __aenter__(self):
            return SimpleNamespace(chromium=SimpleNamespace(connect_over_cdp=connect), stop=stop)
    async def no_http(url): pytest.fail('Websocket endpoints have no REST discovery URL')
    monkeypatch.setattr(module, 'async_playwright', Manager)
    monkeypatch.setattr(module, '_async_cdp_targets', no_http)
    backend = module.CDPBackend()
    if connects:
        assert await backend._ensure_browser() is browser
        assert calls == [endpoint]
    else:
        with pytest.raises(ConnectionError): await backend._ensure_browser()
        assert calls == [endpoint, 'stop'] and backend._playwright is None


@pytest.mark.asyncio
async def test_reconnect_refuses_a_different_target_with_the_same_url(monkeypatch):
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    class Page:
        url = 'https://example.test/form'
        def __init__(self, target): self.target_id = target
        def is_closed(self): return False
    backend = CDPBackend()
    original, user = Page('task-target'), Page('user-target')
    backend._pages['確認'] = original
    backend._owned_pages().add(id(original))
    async def connect(): return SimpleNamespace(contexts=[SimpleNamespace(pages=[user])])
    monkeypatch.setattr(backend, '_ensure_browser', connect)
    with pytest.raises(ConnectionError): await backend.reconnect('確認')
    with pytest.raises(ConnectionError): await backend._get_page('確認')
    assert '確認' not in backend._pages


@pytest.mark.asyncio
async def test_reconnect_retains_target_after_navigation_and_preserves_ownership(monkeypatch):
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    class Page:
        target_id = 'same-target'
        def __init__(self, url): self.url = url
        def is_closed(self): return False
    backend = CDPBackend()
    original, recovered = Page('https://example.test/old'), Page('https://example.test/redirected')
    backend._pages['確認'] = original
    backend._owned_pages().add(id(original))
    async def connect(): return SimpleNamespace(contexts=[SimpleNamespace(pages=[recovered])])
    monkeypatch.setattr(backend, '_ensure_browser', connect)
    await backend.reconnect('確認')
    assert await backend._get_page('確認') is recovered
    assert id(recovered) in backend._owned_pages()


@pytest.mark.asyncio
async def test_missing_target_identity_is_not_reprobed_on_every_page_operation(monkeypatch):
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    backend = CDPBackend()
    page = SimpleNamespace(is_closed=lambda: False)
    backend._pages['確認'] = page
    attempts = []
    async def identify(candidate):
        attempts.append(candidate)
        return None
    monkeypatch.setattr(backend, '_page_target_id', identify)
    for _ in range(3):
        assert await backend._get_page('確認') is page
    assert attempts == [page]


@pytest.mark.asyncio
async def test_many_stalled_targets_share_one_reconnect_deadline_and_drain(monkeypatch):
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    backend = CDPBackend()
    backend._RECONNECT_PROBE_TIMEOUT = .06
    original = SimpleNamespace(target_id='owned', url='https://example.test', is_closed=lambda: False)
    recovered = SimpleNamespace(target_id='owned', url='https://example.test/new', is_closed=lambda: False)
    others = [SimpleNamespace(target_id=str(i), is_closed=lambda: False) for i in range(80)]
    backend._pages['確認'] = original
    backend._owned_pages().add(id(original))
    active = 0
    maximum = 0
    cancelled = []
    async def identify(page):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            if page is recovered: return page.target_id
            try: await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(page.target_id)
                raise
        finally: active -= 1
    async def connect(): return SimpleNamespace(contexts=[SimpleNamespace(pages=[recovered, *others])])
    monkeypatch.setattr(backend, '_page_target_id', identify)
    monkeypatch.setattr(backend, '_ensure_browser', connect)
    started = asyncio.get_running_loop().time()
    await backend.reconnect('確認')
    assert asyncio.get_running_loop().time() - started < .5
    assert backend._pages['確認'] is recovered
    assert id(recovered) in backend._owned_pages()
    assert 1 < maximum <= backend._RECONNECT_PROBE_CONCURRENCY
    assert cancelled and active == 0


@pytest.mark.asyncio
async def test_resolved_session_survives_configuration_reload_and_wait_timeout(monkeypatch):
    from nullion.plugins.browser_plugin.backends import auto_backend as module
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    calls = []
    class Connected:
        BACKEND_NAME = 'cdp'
        _cdp_url = 'http://127.0.0.1:19222'
        async def wait_for(self, session_id):
            raise TimeoutError('条件がまだ一致しません')
    backend = module.AutoBackend()
    backend._backend = Connected()
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://127.0.0.1:1')
    def reachable(host, port):
        calls.append((host, port))
        return port == 19222
    monkeypatch.setattr(module, '_is_cdp_reachable_at', reachable)
    wrapped = SessionRecoveryBackend(backend)
    assert await wrapped.browser_task_connection('確認') == ('http://127.0.0.1:19222', True)
    with pytest.raises(TimeoutError, match='条件'):
        await backend._call('wait_for', '確認')
    assert calls == [('127.0.0.1', 19222)]


@pytest.mark.asyncio
async def test_direct_cdp_task_connection_uses_captured_endpoint(monkeypatch):
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://127.0.0.1:19222')
    backend = CDPBackend()
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', 'http://127.0.0.1:1')
    assert await backend.browser_task_connection('comparación') == ('http://127.0.0.1:19222', True)

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

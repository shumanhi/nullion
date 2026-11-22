"""Real native browser attach/reconnect boundaries with disposable profiles."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container browser E2E')


@pytest.mark.parametrize('scheme,port', [('http', 80), ('ws', 80), ('https', 443), ('wss', 443)])
@pytest.mark.parametrize('host', ['configured.test', 'localhost'])
def test_saved_explicit_endpoint_and_backend_resolution_agree(tmp_path, monkeypatch, scheme, port, host):
    from fastapi.testclient import TestClient
    from nullion import web_app, auth
    from nullion.plugins.browser_plugin.backends.auto_backend import _cdp_endpoint
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    env_path = tmp_path / '.env'
    saved = {}
    monkeypatch.setenv('NULLION_ENV_FILE', str(env_path))
    monkeypatch.setattr(web_app, '_find_env_path', lambda: env_path)
    monkeypatch.setattr(web_app, '_read_credentials_json', lambda: dict(saved))
    monkeypatch.setattr(auth, 'load_stored_credentials', lambda: dict(saved))
    def save(credentials):
        saved.clear()
        saved.update(credentials)
    monkeypatch.setattr(web_app, '_write_credentials_json', save)
    endpoint = f'{scheme}://{host}/opaque'
    expected_host = '127.0.0.1' if host == 'localhost' else host
    expected_url = f'{scheme}://127.0.0.1:{port}/opaque' if host == 'localhost' else endpoint
    with TestClient(web_app.create_app(runtime, orchestrator=None, registry=ToolRegistry())) as client:
        response = client.post('/api/config', json={'browser_backend': 'auto',
            'browser_preferred': 'brave', 'browser_cdp_url': endpoint})
        assert response.status_code == 200, response.text
        assert response.json()['ok'] is True
        visible = client.get('/api/config').json()
        assert visible['browser_cdp_url'] == expected_url
    assert saved['browser_cdp_url'] == expected_url
    assert f'NULLION_BROWSER_CDP_URL="{expected_url}"' in env_path.read_text()
    assert _cdp_endpoint() == (expected_host, port)
    assert CDPBackend()._cdp_url == expected_url


@pytest.mark.parametrize('endpoint_kind', ['http', 'ws'])
def test_native_attach_retains_exact_target_and_never_adopts_same_url_user_tab(tmp_path, monkeypatch, endpoint_kind):
    from playwright.sync_api import sync_playwright
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools, _run
    from nullion.tools import ToolInvocation
    from tests.e2e.test_web_surface_contracts import _free_port

    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write('<html><title>同じページ</title><input id="field"><main>接続済み 37</main></html>'.encode())
        def log_message(self, *args): pass

    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{source.server_port}/form'
    port = _free_port()
    http_endpoint = f'http://127.0.0.1:{port}'
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'cdp')
    try:
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(str(tmp_path / 'profile'), headless=True,
                args=[f'--remote-debugging-port={port}'])
            try:
                user_page = context.pages[0]
                user_page.goto(url)
                endpoint = http_endpoint
                if endpoint_kind == 'ws':
                    with urlopen(http_endpoint + '/json/version') as response:
                        endpoint = json.load(response)['webSocketDebuggerUrl']
                monkeypatch.setenv('NULLION_BROWSER_CDP_URL', endpoint)
                backend = CDPBackend()
                recovered = SessionRecoveryBackend(backend)
                policy = BrowserPolicy()
                policy.block_private = False
                tools = BrowserTools(recovered, BrowserSessionPool(), policy)
                def invocation(name, arguments):
                    return ToolInvocation('native-' + name, name, 'web:operator',
                        {'session_id': '接続確認', **arguments})
                try:
                    result = tools.browser_navigate(invocation('browser_navigate', {'url': url}))
                    assert result.status == 'completed', result.error
                    session_id = result.output['session_id']
                    target_id = backend._page_targets()[session_id]
                    assert target_id
                    # Both pages have the same URL. Recovery must select the
                    # original native target, retaining form state and ownership.
                    _run(backend.run_js(session_id, "() => { document.querySelector('#field').value = '37'; }"))
                    _run(backend.reconnect(session_id))
                    assert _run(backend.run_js(session_id, "() => document.querySelector('#field').value")) == '37'
                    assert id(backend._pages[session_id]) in backend._owned_pages()
                    assert user_page.locator('#field').input_value() == ''
                    # Removing that exact task target must not transfer later
                    # interactions to the user's otherwise identical page.
                    session = context.new_cdp_session(user_page)
                    session.send('Target.closeTarget', {'targetId': target_id})
                    session.detach()
                    with pytest.raises(ConnectionError):
                        _run(backend.reconnect(session_id))
                    failed = tools.browser_type(invocation('browser_type', {'selector': '#field', 'text': 'unexpected'}))
                    assert failed.status == 'failed'
                    assert failed.output['requires_user_choice'] is True
                    assert not user_page.is_closed()
                    assert user_page.url == url
                    assert user_page.locator('#field').input_value() == ''
                finally:
                    _run(backend.shutdown())
            finally:
                context.close()
    finally:
        source.shutdown()
        source.server_close()

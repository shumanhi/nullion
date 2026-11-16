import pytest
from scripts.live_stage_browser_guard import validate_browser


@pytest.mark.parametrize('runtime,allowed',[
    ({'browser_backend':'playwright'},False),
    ({'browser_backend':'cdp','browser_cdp_url':'http://127.0.0.1:19224'},False),
    ({'browser_backend':'cdp','browser_cdp_url':'http://127.0.0.1:19226'},True),
])
def test_live_guard_uses_effective_runtime_settings_instead_of_stale_env(runtime,allowed):
    from scripts.live_stage_browser_guard import effective_browser_config
    config=effective_browser_config({'NULLION_BROWSER_BACKEND':'auto',
        'NULLION_BROWSER_CDP_URL':'http://127.0.0.1:19226'},runtime)
    owner='/Applications/Brave Browser.app/Contents/MacOS/Brave Browser'
    if config['NULLION_BROWSER_CDP_URL'].endswith(':19224'):
        owner='/Applications/Docker.app/Contents/MacOS/com.docker.backend'
    if allowed:validate_browser(config,system='Darwin',owner=owner,listening=True)
    else:
        with pytest.raises(ValueError):validate_browser(config,system='Darwin',owner=owner,listening=True)


@pytest.mark.parametrize('owner,preferred,backend,headless,listening,allowed',[
    ('/Applications/Brave Browser.app/Contents/MacOS/Brave Browser','brave','auto','false',True,True),
    ('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome','brave','cdp','false',True,False),
    ('/Applications/Docker.app/Contents/MacOS/com.docker.backend','brave','cdp','false',True,False),
    (None,'brave','auto','false',False,True),
    (None,'brave','cdp','false',False,False),
    (None,'brave','playwright','true',False,False),
])
def test_live_guard_checks_selected_native_browser(owner,preferred,backend,headless,listening,allowed):
    config={'NULLION_BROWSER_BACKEND':backend,'NULLION_BROWSER_PREFERRED':preferred,
        'NULLION_BROWSER_HEADLESS':headless,'NULLION_BROWSER_CDP_URL':'http://127.0.0.1:19226'}
    if allowed:
        validate_browser(config,system='Darwin',owner=owner,listening=listening)
    else:
        with pytest.raises(ValueError):validate_browser(config,system='Darwin',owner=owner,listening=listening)


@pytest.mark.parametrize('available,matching',[(False,False),(True,False),(True,True)])
def test_guard_cli_distinguishes_unavailable_api_from_changed_settings(tmp_path,available,matching):
    import hashlib,json,subprocess,sys,threading
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    from pathlib import Path
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.end_headers();self.wfile.write(b'{"browser_backend":"auto"}')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);port=server.server_port
    if available:threading.Thread(target=server.serve_forever,daemon=True).start()
    else:server.server_close()
    expected=hashlib.sha256(json.dumps({'NULLION_BROWSER_BACKEND':'auto'},sort_keys=True).encode()).hexdigest() if matching else 'different'
    try:
        result=subprocess.run([sys.executable,str(Path(__file__).parents[1]/'scripts/live_stage_browser_guard.py'),
            '--stage-home',str(tmp_path),'--base-url',f'http://127.0.0.1:{port}',
            '--expected-snapshot',expected,'verify-unchanged'],capture_output=True,text=True,timeout=10)
        assert result.returncode==(0 if matching else 1),result.stderr
        if not available:
            assert 'Could not verify' in result.stderr
            assert 'settings changed' not in result.stderr
        elif not matching:assert 'settings changed' in result.stderr
    finally:
        if available:server.shutdown();server.server_close()

"""The source-browser fixture defaults to headed Chrome on a virtual display."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker browser boundary')


@pytest.mark.parametrize('headless', [False, True])
def test_source_browser_fixture_uses_explicit_execution_mode(headless):
    from playwright.sync_api import sync_playwright
    script = Path(__file__).parent/'fixtures/live_cdp_service.py'
    process = subprocess.Popen([sys.executable, str(script), *(['--headless'] if headless else [])],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic()+20
        while True:
            assert process.poll() is None, 'Source browser fixture exited during startup'
            try:
                with urllib.request.urlopen('http://127.0.0.1:19224/json/version', timeout=1) as r:
                    version = json.load(r)
                break
            except OSError:
                assert time.monotonic()<deadline, 'Source browser fixture did not become ready'
                time.sleep(.1)
        assert ('HeadlessChrome' in version['User-Agent']) is headless, version
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp('http://127.0.0.1:19224')
            page = browser.contexts[0].new_page()
            try:
                page.set_content('<h1>確認済み — Verificado</h1>')
                assert page.locator('h1').inner_text() == '確認済み — Verificado'
                assert page.screenshot().startswith(b'\x89PNG\r\n\x1a\n')
            finally:
                page.close()
                browser.close()
    finally:
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)

"""Run the optional browser dependency in its own Python environment.

Only model requests cross the authenticated loopback bridge; the parent keeps
its configured model client. Browser Use SDK pins never alter that environment.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import secrets
import site
import signal
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


def _signal_worker(process, *, force=False):
    if platform.system() == 'Windows':
        subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=5)
    else:
        os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)


def run_isolated_browser_task(task, *, client, principal_id, policy, timeout, python=None):
    token = secrets.token_urlsafe(32)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            if self.path != '/model' or self.headers.get('Authorization') != 'Bearer ' + token:
                self.send_error(403)
                return
            length = int(self.headers.get('Content-Length', 0))
            if not 0 < length < 16_000_000:
                self.send_error(413)
                return
            try:
                data = json.loads(self.rfile.read(length))
                result = client.create(messages=data['messages'], tools=[], system=data.get('system'), max_tokens=8192, timeout=120)
                encoded = json.dumps(result, default=str).encode()
                self.send_response(200)
            except Exception:
                encoded = b'{"error":"Configured model request failed"}'
                self.send_response(502)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            try:
                self.wfile.write(encoded)
            except (BrokenPipeError, ConnectionResetError):
                pass

    bridge = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    # A cancelled worker can leave an external model request in flight.
    # Closing its bridge must not wait beyond the tool's own deadline.
    bridge.daemon_threads = True
    thread = threading.Thread(target=bridge.serve_forever, daemon=True)
    thread.start()
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix='nullion-browser-worker-') as scratch:
            job = Path(scratch) / 'job.json'
            result_file = Path(scratch) / 'result.json'
            job.write_text(json.dumps({'task': task.model_dump(), 'principal_id': principal_id, 'timeout': timeout,
                'fallback_site_packages': site.getsitepackages(), 'model': str(getattr(client, 'model', 'configured-model')), 'bridge_url': f'http://127.0.0.1:{bridge.server_port}/model', 'token': token,
                'policy': {'allowed_domains': list(policy.allowed_domains), 'blocked_domains': list(policy.blocked_domains), 'block_private': policy.block_private}}, ensure_ascii=False))
            job.chmod(0o600)
            env = {key: value for key, value in os.environ.items() if key in {'PATH', 'HOME', 'TMPDIR', 'DISPLAY', 'XAUTHORITY', 'PLAYWRIGHT_BROWSERS_PATH', 'NULLION_HOME', 'NULLION_DATA_DIR', 'NULLION_CHECKPOINT_PATH'} or key.startswith(('NULLION_BROWSER_', 'NULLION_WORKSPACE_'))}
            env.update(PYTHONPATH=str(Path(__file__).resolve().parents[3]), ANONYMIZED_TELEMETRY='false', PYTHONUNBUFFERED='1')
            env.pop('NULLION_BROWSER_USE_PYTHON', None)
            with (Path(scratch) / 'worker.log').open('wb') as log:
                process = subprocess.Popen([python or os.environ['NULLION_BROWSER_USE_PYTHON'], str(Path(__file__).resolve()), str(job), str(result_file)], env=env, stdout=log, stderr=log, start_new_session=True)
                process.wait(timeout=timeout + 25)
            if process.returncode != 0 or not result_file.is_file():
                raise RuntimeError('Isolated browser worker failed before producing a report')
            result = json.loads(result_file.read_text())
            if 'worker_error' in result:
                raise RuntimeError('Isolated browser worker: ' + result['worker_error']['type'] + ': ' + result['worker_error']['message'])
            logger.info('Browser Use worker report status=%s observations=%s rejected=%s elapsed_seconds=%s', result.get('report_status'), len(result.get('observations', [])), result.get('rejected_observations'), result.get('elapsed_seconds'))
            return result
    finally:
        if process is not None and process.poll() is None:
            _signal_worker(process)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _signal_worker(process, force=True)
                process.wait(timeout=5)
        bridge.shutdown()
        bridge.server_close()
        thread.join(timeout=2)


class BridgeModelClient:
    def __init__(self, job):
        self.model = job['model']
        self.url = job['bridge_url']
        self.token = job['token']

    def create(self, *, messages, tools, system, max_tokens, timeout):
        request = Request(self.url, data=json.dumps({'messages': messages, 'system': system}).encode(), headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        with urlopen(request, timeout=timeout + 5) as response:
            return json.load(response)


async def _worker(job):
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask, run_browser_task
    policy = BrowserPolicy()
    policy.allowed_domains = frozenset(job['policy']['allowed_domains'])
    policy.blocked_domains = frozenset(job['policy']['blocked_domains'])
    policy.block_private = job['policy']['block_private']
    return await run_browser_task(BrowserTask.model_validate(job['task']), client=BridgeModelClient(job), principal_id=job['principal_id'], policy=policy, timeout=job['timeout'])


if __name__ == '__main__':
    job = json.loads(Path(sys.argv[1]).read_text())
    # Prefer this worker's isolated dependencies; reuse app-only packages
    # from the parent environment only when they are absent here.
    sys.path.extend(path for path in job['fallback_site_packages'] if path not in sys.path)
    try:
        result = asyncio.run(_worker(job))
    except Exception as exc:
        result = {'worker_error': {'type': type(exc).__name__, 'message': str(exc)[:300]}}
    Path(sys.argv[2]).write_text(json.dumps(result, ensure_ascii=False))

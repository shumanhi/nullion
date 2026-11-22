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
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


def _signal_worker(process, *, force=False):
    if platform.system() == 'Windows':
        subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=5)
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
        except PermissionError:
            if process.poll() is None:
                raise
            # A denied group signal after the child exits is a cleanup issue,
            # not a failed browser task. Preserve its report and continue the
            # independent owned-tab ledger cleanup in the caller's finally.
            logger.warning('Exited browser worker group cleanup denied pid=%s', process.pid)


def run_isolated_browser_task(task, *, client, principal_id, policy, timeout, python=None, progress_callback=None, browser_mode="configured", browser_connection=None):
    token = secrets.token_urlsafe(32)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            if self.path not in {'/model', '/progress'} or self.headers.get('Authorization') != 'Bearer ' + token:
                self.send_error(403)
                return
            length = int(self.headers.get('Content-Length', 0))
            if not 0 < length < 16_000_000:
                self.send_error(413)
                return
            try:
                data = json.loads(self.rfile.read(length))
                if self.path == '/progress':
                    counts = {key: data.get(key) for key in ('pages_checked', 'findings_found')}
                    if not all(type(value) is int and 0 <= value <= 1_000_000 for value in counts.values()):
                        self.send_error(400)
                        return
                    from nullion.request_progress import SOURCE_ACTIVITY_PHASES
                    if isinstance(data.get('activity_phase'), str) and data['activity_phase'] in SOURCE_ACTIVITY_PHASES:
                        counts['activity_phase'] = data['activity_phase']
                    if progress_callback is not None:
                        try:
                            progress_callback(counts)
                        except Exception:
                            logger.debug('Browser progress callback unavailable', exc_info=True)
                    self.send_response(204)
                    self.end_headers()
                    return
                result = client.create(messages=data['messages'], tools=data.get('tools') or [], system=data.get('system'), max_tokens=8192, timeout=120)
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
    from nullion.plugins.browser_plugin.browser_resources import browser_resource_directory, browser_temporary_root
    from nullion.plugins.browser_plugin.browser_config import nullion_runtime_home
    process = None
    result = None
    try:
        with browser_resource_directory(nullion_runtime_home() / "browser-task-scratch") as scratch, browser_resource_directory(browser_temporary_root()) as sdk_temp:
            job = Path(scratch) / 'job.json'
            result_file = Path(scratch) / 'result.json'
            from uuid import uuid4
            tab_lease_path = nullion_runtime_home() / 'browser-tab-leases' / (uuid4().hex + '.json')
            from nullion.tools import tool_execution_owner
            job.write_text(json.dumps({'task': task.model_dump(), 'principal_id': principal_id, 'timeout': timeout, 'browser_mode': browser_mode, 'browser_connection': browser_connection,
                'owner': tool_execution_owner(),
                'fallback_site_packages': site.getsitepackages(), 'model': str(getattr(client, 'model', 'configured-model')), 'bridge_url': f'http://127.0.0.1:{bridge.server_port}/model', 'token': token,
                'resource_lease': str(Path(scratch) / '.lease'),
                'tab_lease_path': str(tab_lease_path),
                'policy': {'allowed_domains': list(policy.allowed_domains), 'blocked_domains': list(policy.blocked_domains), 'block_private': policy.block_private}}, ensure_ascii=False))
            job.chmod(0o600)
            env = {key: value for key, value in os.environ.items() if key in {'PATH', 'HOME', 'TMPDIR', 'DISPLAY', 'XAUTHORITY', 'PLAYWRIGHT_BROWSERS_PATH', 'NULLION_HOME', 'NULLION_DATA_DIR', 'NULLION_CHECKPOINT_PATH'} or key.startswith(('NULLION_BROWSER_', 'NULLION_WORKSPACE_'))}
            env.update(TMPDIR=str(sdk_temp), TEMP=str(sdk_temp), TMP=str(sdk_temp), PYTHONPATH=str(Path(__file__).resolve().parents[3]), ANONYMIZED_TELEMETRY='false', PYTHONUNBUFFERED='1')
            env.pop('NULLION_BROWSER_USE_PYTHON', None)
            try:
                with (Path(scratch) / 'worker.log').open('wb') as log:
                    process = subprocess.Popen([python or os.environ['NULLION_BROWSER_USE_PYTHON'], str(Path(__file__).resolve()), str(job), str(result_file)], env=env, stdout=log, stderr=log, start_new_session=True)
                    from nullion.tools import tool_execution_cancelled
                    deadline = time.monotonic() + timeout + 25
                    published=None
                    while process.poll() is None:
                        if tool_execution_cancelled():
                            raise InterruptedError('Browser task was cancelled by its owning request')
                        if result_file.is_file():
                            try:published=json.loads(result_file.read_text())
                            except (OSError,ValueError):published=None
                            if isinstance(published,dict):break
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise subprocess.TimeoutExpired(process.args, timeout + 25)
                        try:
                            process.wait(timeout=min(.25, remaining))
                        except subprocess.TimeoutExpired:
                            pass
                # A worker can publish and exit between polls. Native report
                # publication and SDK shutdown are separate outcomes. Owner
                # cancellation wins even when the child has already exited.
                if tool_execution_cancelled():
                    raise InterruptedError('Browser task was cancelled by its owning request')
                if published is None and not result_file.is_file():
                    raise RuntimeError('Isolated browser worker failed before producing a report')
                result=published if published is not None else json.loads(result_file.read_text())
                if not isinstance(result,dict):raise RuntimeError('Invalid isolated browser report')
                if process.returncode not in (None,0):
                    logger.warning('Browser worker exited with code=%s after publishing its native report',process.returncode)
                if 'worker_error' in result:
                    if result['worker_error'].get('reason') == 'storage_exhausted':
                        import errno
                        raise OSError(errno.ENOSPC, 'Browser storage is full')
                    raise RuntimeError('Isolated browser worker: ' + result['worker_error']['type'] + ': ' + result['worker_error']['message'])
                logger.info('Browser Use worker report status=%s observations=%s rejected=%s elapsed_seconds=%s', result.get('report_status'), len(result.get('observations', [])), result.get('rejected_observations'), result.get('elapsed_seconds'))
                return result
            finally:
                if process is not None:
                    try:
                        # An exited Windows PID can be reused; taskkill cannot
                        # identify its old descendants after the parent exits.
                        if process.poll() is None or platform.system() != "Windows":
                            _signal_worker(process, force=process.poll() is not None)
                    except ProcessLookupError:
                        pass
                    if process.poll() is None:
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            _signal_worker(process, force=True)
                            process.wait(timeout=5)
                # The child may be killed before its finally block or SDK
                # shutdown completes. Its private target ledger survives it.
                from nullion.plugins.browser_plugin.browser_tab_lease import cleanup_worker_tab_lease
                from nullion.plugins.browser_plugin.browser_tools import _run
                try:
                    cleanup = _run(cleanup_worker_tab_lease(tab_lease_path), timeout_seconds=10)
                    if cleanup is not None and isinstance(result, dict):
                        result['browser_tab_cleanup'] = cleanup
                except Exception:
                    logger.warning('Worker tab cleanup remains pending', exc_info=True)
    finally:
        bridge.shutdown()
        bridge.server_close()
        thread.join(timeout=2)


class BridgeModelClient:
    def __init__(self, job):
        self.model = job['model']
        self.url = job['bridge_url']
        self.token = job['token']

    def create(self, *, messages, tools, system, max_tokens, timeout):
        request = Request(self.url, data=json.dumps({'messages': messages, 'tools': tools, 'system': system}).encode(), headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        with urlopen(request, timeout=timeout + 5) as response:
            return json.load(response)

    def progress(self, counts):
        request = Request(self.url.rsplit('/', 1)[0] + '/progress', data=json.dumps(counts).encode(),
            headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        try:
            with urlopen(request, timeout=1):
                pass
        except Exception:
            # Optional progress can never fail the source lookup.
            pass


def _publish_worker_result(path,result):
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_text(json.dumps(result,ensure_ascii=False))
    temporary.chmod(0o600)
    os.replace(temporary,path)


async def _worker(job,result_path=None):
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask, run_browser_task
    policy = BrowserPolicy()
    policy.allowed_domains = frozenset(job['policy']['allowed_domains'])
    policy.blocked_domains = frozenset(job['policy']['blocked_domains'])
    policy.block_private = job['policy']['block_private']
    try:
        client = BridgeModelClient(job)
        connection_args = {'browser_connection': job['browser_connection']} if job.get('browser_connection') is not None else {}
        result=await run_browser_task(BrowserTask.model_validate(job['task']), client=client, principal_id=job['principal_id'], policy=policy, timeout=job['timeout'], progress_callback=client.progress, browser_mode=job.get("browser_mode", "configured"), tab_lease_path=job.get('tab_lease_path'), **connection_args)
    except Exception as exc:
        from nullion.tools import filesystem_error_metadata
        result={'worker_error':{'type':type(exc).__name__,'message':str(exc)[:300],
            **filesystem_error_metadata(exc,reason='browser_task_unavailable')}}
    # Publish before asyncio.run drains third-party background tasks. A
    # cancellation-resistant SDK cleanup cannot erase this native report.
    if result_path is not None:_publish_worker_result(result_path,result)
    return result


if __name__ == '__main__':
    job = json.loads(Path(sys.argv[1]).read_text())
    # Prefer this worker's isolated dependencies; reuse app-only packages
    # from the parent environment only when they are absent here.
    sys.path.extend(path for path in job['fallback_site_packages'] if path not in sys.path)
    try:
        from portalocker import Lock, LOCK_SH
        with Lock(job["resource_lease"], mode="r+", flags=LOCK_SH, timeout=0):
            result = asyncio.run(_worker(job,Path(sys.argv[2])))
    except Exception as exc:
        from nullion.tools import filesystem_error_metadata
        result = {'worker_error': {'type': type(exc).__name__, 'message': str(exc)[:300], **filesystem_error_metadata(exc, reason='browser_task_unavailable')}}
    if not Path(sys.argv[2]).is_file():_publish_worker_result(Path(sys.argv[2]),result)

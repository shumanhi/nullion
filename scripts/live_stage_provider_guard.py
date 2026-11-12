"""Stop paid live probes after fresh, terminal provider evidence. No model calls."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import threading
from concurrent.futures import Future
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def terminal_provider_event(record):
    """Return safe facts only; prompts and legacy message_text are not diagnostics."""
    if record.get('event_type') in ('conversation.model_failure', 'model.provider_health'):
        detail = record.get('provider_error') or (record.get('payload') or {}).get('provider_error') or {}
        if isinstance(detail, dict) and detail.get('terminal') is True and detail.get('category') in ('quota', 'auth'):
            return {k: detail[k] for k in ('category', 'terminal', 'http_status', 'error_type', 'provider_code') if k in detail}
        return None
    if record.get('event_type') != 'doctor.signal_received':
        return None
    reason = str((record.get('payload') or {}).get('reason') or '')
    diagnostic = ''
    offset = 0
    for part in reason.split(';'):
        key, separator, value = part.partition('=')
        if key.strip() == 'message_text':
            break
        if separator and key.strip() in ('error', 'detail', 'backend_detail'):
            diagnostic = reason[offset + len(key) + 1:]
            break
        offset += len(part) + 1
    diagnostic_prefix = diagnostic.split(';message_text=', 1)[0]
    status = re.search(r'\bHTTP\s+(\d{3})\b', diagnostic_prefix)
    if not status:
        return None
    try:
        start = diagnostic.index('{')
        if start >= len(diagnostic_prefix):
            return None
        payload, _ = json.JSONDecoder().raw_decode(diagnostic[start:])
    except (ValueError, TypeError):
        return None
    error = payload.get('error') if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return None
    code = error.get('type') or error.get('code')
    provider_code = error.get('code')
    http_status = int(status.group(1))
    category = ('quota' if http_status == 429 and code in ('usage_limit_reached', 'insufficient_quota')
                else 'auth' if http_status == 401 and any(value in ('token_invalidated', 'token_revoked', 'invalid_api_key', 'invalid_token')
                    for value in (code,provider_code)) else None)
    result = {'category': category, 'terminal': True, 'http_status': http_status, 'error_type': code} if category else None
    if result and isinstance(provider_code,str):result['provider_code']=provider_code
    return result


class LiveStageProviderGuard:
    def __init__(self, *, since=None):
        self.since = since or datetime.now(timezone.utc).isoformat()
        self._pending_check = None

    def check(self):
        connection = os.environ.get('NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION')
        if not connection:
            return None
        settings = json.loads(Path(connection).read_text())
        request = Request('http://host.docker.internal:' + str(settings['port']) + '/model-health?' + urlencode({'since': self.since}),
                          headers={'Authorization': 'Bearer ' + settings['token']})
        try:
            from scripts.live_stage_http import read_json
        except ModuleNotFoundError:
            from live_stage_http import read_json
        events = read_json(request, opener=urlopen, retry_seconds=30)['events']
        if not events:
            return None
        event = events[-1]
        output = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR', '/tmp/live-web-evidence'))
        output.mkdir(parents=True, exist_ok=True)
        (output / 'provider-blocked.json').write_text(json.dumps(event, indent=2))
        return 'Live stage probes stopped: terminal model provider ' + event['details']['category'] + ' failure (HTTP ' + str(event['details'].get('http_status', 'unknown')) + '). Remaining cases were not attempted.'


    @property
    def check_pending(self):
        return self._pending_check is not None

    def finish_pending(self):
        """Drain the current case's read before another paid case may start."""
        pending = self._pending_check
        self._pending_check = None
        return pending.result(timeout=35) if pending is not None else None

    def check_boundary(self):
        blocked = self.finish_pending()
        return blocked or self.check()

    def poll(self):
        """Observe quota/auth without blocking the browser's event processing.

        Fixture preflight/teardown drain pending reads and check synchronously. In-flight
        polling only retries the same read; it never repeats a paid chat turn.
        Transport errors propagate on the next poll, preserving fail-closed
        cleanup of the owned request.
        """
        if self._pending_check is not None:
            if not self._pending_check.done():
                return None
            completed = self._pending_check
            self._pending_check = None
            return completed.result()
        pending = Future()
        self._pending_check = pending
        def check_in_background():
            try:
                pending.set_result(self.check())
            except BaseException as error:
                pending.set_exception(error)
        threading.Thread(target=check_in_background,
                         name="stage-provider-evidence", daemon=True).start()
        return None


_guard = None


def process_provider_guard():
    global _guard
    if _guard is None:
        _guard = LiveStageProviderGuard()
    return _guard

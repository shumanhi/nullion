"""Bounded read-only transport for native stage evidence; never replays work."""
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def read_json(request, *, timeout=10, retry_seconds=8, opener=None):
    if request.get_method() != 'GET':
        raise ValueError('Stage evidence retries accept GET only')
    open_request = opener or urlopen
    deadline = time.monotonic() + retry_seconds
    while True:
        try:
            with open_request(request, timeout=min(timeout, max(.01, deadline-time.monotonic()))) as response:
                return json.load(response)
        except (URLError, OSError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in {502, 503, 504}:
                raise
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(.2, remaining))

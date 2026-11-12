"""Controlled source changes must be independent from clocks and other owners."""
import json
from http.server import ThreadingHTTPServer
from threading import Thread
from urllib.request import Request, urlopen

from tests.e2e.fixtures.live_source_server import Sources


def test_snapshot_fixture_changes_twice_without_changing_other_owners():
    class OwnedSources(Sources):
        watches = {}

    server = ThreadingHTTPServer(('127.0.0.1', 0), OwnedSources)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    base = 'http://127.0.0.1:' + str(server.server_port)

    def reading(owner):
        with urlopen(base + '/state-watch/data?owner=' + owner, timeout=3) as response:
            return json.load(response)

    try:
        with urlopen(base + '/public-fact/', timeout=3) as response:
            document = response.read()
            assert response.status == 200
            assert b'<h1>Recorded sample facts</h1>' in document
            assert b'visitor' not in document.lower()
        first = reading('one')
        second = reading('one')
        assert first['votes'] == second['votes'] == 10
        assert second['clock'] > first['clock']
        for expected in (11, 12):
            with urlopen(Request(base + '/state-watch/advance?owner=one', data=b'', method='POST'), timeout=3) as response:
                assert response.status == 204
            assert reading('one')['votes'] == expected
            assert reading('two')['votes'] == 10
            with urlopen(base + '/state-watch/?owner=one', timeout=3) as response:
                assert f'Total votes: {expected}'.encode() in response.read()
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)

import io
from urllib.error import HTTPError, URLError
from urllib.request import Request
import pytest
from scripts.live_stage_http import read_json


def test_evidence_retries_transient_read_without_another_paid_request():
    reads=[]
    def opener(request,**kw):
        reads.append(request.full_url)
        if len(reads)==1:raise URLError(OSError(101,'Network is unreachable'))
        return io.BytesIO(b'{"events":[]}')
    assert read_json(Request('http://127.0.0.1/evidence'),opener=opener)=={'events':[]}
    assert len(reads)==2 and len(set(reads))==1


@pytest.mark.parametrize('code',[400,401,403,404])
def test_evidence_never_retries_authorization_or_invalid_request(code):
    reads=[]
    def opener(request,**kw):
        reads.append(True);raise HTTPError(request.full_url,code,'rejected',{},None)
    with pytest.raises(HTTPError):read_json(Request('http://127.0.0.1/evidence'),opener=opener)
    assert len(reads)==1


def test_evidence_refuses_mutations_before_opening_network():
    with pytest.raises(ValueError):read_json(Request('http://127.0.0.1/api/chat',data=b'{}'),opener=lambda *a,**kw:pytest.fail('sent'))


def test_permanent_evidence_outage_is_not_silently_ignored():
    def opener(*a,**kw):raise URLError('offline')
    with pytest.raises(URLError):read_json(Request('http://127.0.0.1/evidence'),retry_seconds=0,opener=opener)


def test_probe_status_get_recovers_against_real_read_only_http_server():
    import threading
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    from scripts.live_stage_chat_contract_probe import _get_json
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(('GET',self.path))
            if len(calls)==1:
                self.send_error(503);return
            self.send_response(200);self.end_headers();self.wfile.write(b'{"status":"ok"}')
        def do_POST(self):pytest.fail('A status retry must never submit work')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        assert _get_json(f'http://127.0.0.1:{server.server_port}/api/status',timeout=5)=={'status':'ok'}
        assert calls==[('GET','/api/status')]*2
    finally:server.shutdown();server.server_close();thread.join(timeout=2)

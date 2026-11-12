"""A local file cannot mask an undeliverable advertised Web artifact."""
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.error import HTTPError
import pytest
from scripts.live_stage_chat_contract_probe import _artifact_bytes

@pytest.mark.parametrize('status',[200,403,404,500])
def test_advertised_artifact_url_is_authoritative_over_local_fallback(tmp_path,status,monkeypatch):
    from scripts import live_stage_chat_contract_probe as probe
    monkeypatch.setattr(probe.time,'sleep',lambda _:None)
    monkeypatch.setenv('NULLION_LIVE_HOST_HEADER','127.0.0.1:8753')
    fixture=tmp_path/'existing.txt';fixture.write_bytes(b'local bytes')
    requests=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            assert self.headers['Host']=='127.0.0.1:8753'
            requests.append(self.path);self.send_response(status);self.end_headers();self.wfile.write(b'wire bytes')
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
    try:
        artifact={'name':fixture.name,'path':str(fixture),'url':'/download'}
        base=f'http://127.0.0.1:{server.server_port}'
        if status==200:assert _artifact_bytes(base,artifact,timeout=2)[0]==b'wire bytes'
        else:
            with pytest.raises(HTTPError) as error:_artifact_bytes(base,artifact,timeout=2)
            assert error.value.code==status
        assert len(requests)==(4 if status==404 else 1)
    finally:server.shutdown();server.server_close();worker.join(2)


@pytest.mark.parametrize('failure',[502,503,504,'connect'])
def test_transient_artifact_read_retries_only_the_download(tmp_path,monkeypatch,failure):
    from scripts import live_stage_chat_contract_probe as probe
    from urllib.error import URLError
    monkeypatch.setattr(probe.time,'sleep',lambda _:None)
    fixture=tmp_path/'existing.txt';fixture.write_bytes(b'wrong local fallback')
    reads=[];opens=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            reads.append(self.path)
            self.send_response(failure if isinstance(failure,int) and len(reads)==1 else 200)
            self.end_headers();self.wfile.write(b'actual download')
        def do_POST(self):pytest.fail('A download retry must not submit work')
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
    original=probe.urllib.request.urlopen
    def opener(request,**kwargs):
        opens.append((request.get_method(),request.full_url))
        if failure=='connect' and len(opens)==1:raise URLError(OSError(101,'Network is unreachable'))
        return original(request,**kwargs)
    monkeypatch.setattr(probe.urllib.request,'urlopen',opener)
    try:
        result=_artifact_bytes(f'http://127.0.0.1:{server.server_port}',{'path':str(fixture),'name':fixture.name,'url':'/download'},timeout=2)
        assert result[0]==b'actual download'
        assert len(opens)==2 and len(set(opens))==1
    finally:server.shutdown();server.server_close();worker.join(2)

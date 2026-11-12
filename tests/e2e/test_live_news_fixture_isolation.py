"""The controlled network source must not contaminate adjacent live checks."""
import json
import os
from http.server import ThreadingHTTPServer
import threading
from urllib.request import Request,urlopen
from uuid import uuid4
import pytest
from tests.e2e.fixtures.live_news_server import News

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker network fixture')

def test_owned_news_changes_are_isolated_and_rendered():
    server=ThreadingHTTPServer(('127.0.0.1',0),News)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    base='http://127.0.0.1:'+str(server.server_port)
    def request(path,data=None):
        r=Request(base+path,data=json.dumps(data).encode() if data is not None else None,
            headers={'Content-Type':'application/json'})
        with urlopen(r,timeout=5) as response:return response.read().decode()
    a,b=uuid4().hex,uuid4().hex
    try:
        request('/reset?owner='+a,{})
        request('/reset?owner='+b,{})
        request('/publish?owner='+a,{'title':'Waterfront new garden','url':'/news/garden-test'})
        assert len(json.loads(request('/data?owner='+a)))==2
        assert len(json.loads(request('/data?owner='+b)))==1
        assert 'Waterfront new garden' in request('/?observation_owner='+a)
        assert 'Waterfront new garden' not in request('/?observation_owner='+b)
        request('/reset?owner='+b,{})
        assert len(json.loads(request('/data?owner='+a)))==2
    finally:
        server.shutdown();server.server_close();thread.join(timeout=5)


def test_live_news_source_pushes_changes_without_background_browser_timers():
    server=ThreadingHTTPServer(('127.0.0.1',0),News)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    base='http://127.0.0.1:'+str(server.server_port);owner=uuid4().hex
    try:
        with urlopen(base+'/events?owner='+owner+'&session=owned-push',timeout=5) as stream:
            assert stream.headers['Content-Type']=='text/event-stream'
            def event():
                while True:
                    line=stream.readline().decode()
                    assert line
                    if line.startswith('data: '):return json.loads(line[6:])
            assert len(event())==1
            request=Request(base+'/publish?owner='+owner,data=json.dumps({'title':'庭園','url':'/news/new-push'}).encode(),
                headers={'Content-Type':'application/json'})
            with urlopen(request,timeout=5) as response:assert response.status==200
            changed=event()
            assert len(changed)==2 and changed[-1]['title']=='庭園'
    finally:
        server.shutdown();server.server_close();thread.join(timeout=5)

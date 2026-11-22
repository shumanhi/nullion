"""Browser observations survive creator cleanup and independent poll workers."""
import json
import os
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker browser/SQLite runner')


@pytest.mark.parametrize('tool,pointer',[('browser_snapshot','/snapshot/title'),('browser_extract_detail','/record/title')])
@pytest.mark.parametrize('closed,opened',[('Agotado','Disponible'),('売り切れ','販売中')])
def test_background_source_reopens_after_creator_cleanup_and_worker_restart(tmp_path,monkeypatch,tool,pointer,closed,opened):
    from nullion.live_tracking import LiveTrackerStore,LiveTrackerEngine,utcnow
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry,ToolInvocation
    from tests.e2e.test_scheduled_report_completion import setup_runtime
    state={'title':closed,'loads':0}
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            state['loads']+=1
            content=(f'<!doctype html><title>{state["title"]}</title><main><h1>{state["title"]}</h1>'
                f'<p itemprop="description">{state["title"]}</p><span class="price">$25.00</span></main>').encode()
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8')
            self.send_header('Content-Length',str(len(content)));self.end_headers();self.wfile.write(content)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright');monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    runtime=setup_runtime(tmp_path,monkeypatch);store=LiveTrackerStore(tmp_path/'runtime.db')
    creator=ToolRegistry();register_browser_tools(creator);register_live_tracker_tools(creator)
    creator.live_tracker_store_path=store.path
    owner='web:source-replay';context={'conversation_id':owner,'browser_session_scope':'creator-request'}
    url=f'http://127.0.0.1:{server.server_port}/tickets'
    step={'tool_name':tool,'arguments':{'session_id':'creator-only','ready_selector':'h1'}}
    try:
        nav=creator.invoke(ToolInvocation('nav','browser_navigate',owner,{'url':url,'session_id':'creator-only'},flow_context=context))
        assert nav.status=='completed',nav
        observed=creator.invoke(ToolInvocation('read',tool,owner,step['arguments'],flow_context=context))
        assert observed.status=='completed',observed
        assert observed.output['observation_setup']==[{'tool_name':'browser_navigate','arguments':{'url':url}}, {'tool_name':'browser_wait_for','arguments':{'selector':'h1'}}]
        create=creator.invoke(ToolInvocation('create','create_live_tracker',owner,{
            'name':closed,'duration_seconds':180,'interval_seconds':5,'probe':[step],
            'matcher':{'kind':'structured','conditions':[{'step':0,'pointer':pointer,'op':'equals','value':opened}], 'summary':opened}},
            flow_context={**context,'prepared_observations':{json.dumps(step,sort_keys=True):observed.output}}))
        assert create.status=='completed',create
        ident=create.output['id'];job=store.get(ident)
        assert job['probe_contexts'][0]['observation_setup']
        assert 'browser_session_owner' not in job['probe_contexts'][0]
        store.confirm_start_receipt(ident,job['expires_at']);creator.run_cleanup_hooks()
        for index in range(4):
            if index==3:state['title']=opened
            worker=ToolRegistry();register_browser_tools(worker)
            engine=LiveTrackerEngine(store,registry=worker,runtime_store=runtime.store,deliver=lambda *args:True)
            try:
                engine.check_once(*store.claim(1,now=utcnow()+timedelta(seconds=10)))
                job=store.get(ident)
                assert job['last_outcome']==('matched' if index==3 else 'no_match'),job.get('last_problem')
                assert job['consecutive_errors']==0
            finally:worker.run_cleanup_hooks()
        notice=store.claim_notice();assert notice and notice['kind']=='matched' and opened in notice['text']
        assert state['loads']>=5
    finally:
        creator.run_cleanup_hooks();server.shutdown();server.server_close()

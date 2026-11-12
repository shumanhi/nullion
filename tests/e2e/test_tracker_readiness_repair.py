"""A fresh browser repairs a stale readiness locator and keeps change history."""
import json
import os
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker native browser/SQLite runner')


@pytest.mark.parametrize('label',['現在の集計','Resultados actuales'])
def test_worker_repairs_inline_readiness_and_alerts_on_the_next_changed_value(tmp_path,monkeypatch,label):
    from nullion.live_tracking import LiveTrackerStore,LiveTrackerEngine,utcnow,snapshot_plan_binding
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry,ToolInvocation
    from tests.e2e.test_scheduled_report_completion import setup_runtime

    value={'reading':23,'loads':0}
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path!='/results':self.send_error(404);return
            value['loads']+=1
            body=(f'<h3 id="load-{value["loads"]}">{label}</h3>'
                f'<main id="reading">{value["reading"]}</main>').encode()
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    source=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=source.serve_forever,daemon=True).start()
    for key,text in {'NULLION_ENV_FILE':str(tmp_path/'none.env'),'NULLION_BROWSER_BACKEND':'playwright',
        'NULLION_BROWSER_BLOCK_PRIVATE':'false','NULLION_MEMORY_ENABLED':'0'}.items():monkeypatch.setenv(key,text)
    runtime=setup_runtime(tmp_path,monkeypatch)
    creator,worker=ToolRegistry(),ToolRegistry()
    register_browser_tools(creator);register_browser_tools(worker);register_live_tracker_tools(creator)
    store=LiveTrackerStore(tmp_path/'runtime.db');creator.live_tracker_store_path=store.path
    owner='web:readiness-repair';context={'conversation_id':owner,'browser_session_scope':'creator'}
    url=f'http://127.0.0.1:{source.server_port}/results'
    args={'selector':'#reading','ready_selector':'#load-1','ready_text':label,'session_id':'seed'}
    step={'tool_name':'browser_extract_text','arguments':args}
    class Provider:
        repairs=0;history=[];packets=[]
        def create(self,**kwargs):
            packet=json.loads(kwargs['messages'][0]['content'][0]['text'])
            if kwargs.get('system','').startswith('Repair only the saved READ observation'):
                self.repairs+=1
                self.packets.append(packet)
                assert packet['original_observation']['arguments']['ready_selector']=='#load-1'
                assert packet['inspections'] and label in json.dumps(packet['inspections'],ensure_ascii=False)
                result={'scope_preserved':True,'updates':{'ready_selector':'h3'}}
            else:
                history=packet['snapshot_history'];current=packet['observations'][0]['text']
                self.history.append(history['baseline_established'])
                changed=history['baseline_established'] and current!=history['previous_observations'][0]['text']
                result=({'status':'matched','summary':label+': '+current,
                    'evidence':[{'step':0,'pointer':'/text','kind':'value','value':current}]}
                    if changed else {'status':'no_match'})
            return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps(result,ensure_ascii=False)}]}
    model=Provider()
    try:
        assert creator.invoke(ToolInvocation('open','browser_navigate',owner,{'url':url,'session_id':'seed'},flow_context=context)).status=='completed'
        observed=creator.invoke(ToolInvocation('read','browser_extract_text',owner,args,flow_context=context))
        assert observed.status=='completed' and observed.output['text']=='23'
        created=creator.invoke(ToolInvocation('create','create_live_tracker',owner,{'name':label,
            'duration_seconds':180,'interval_seconds':5,'stop_after_match':False,'probe':[step],
            'matcher':{'kind':'semantic','history_mode':'snapshot','instruction':label}},
            flow_context={**context,'prepared_observations':{json.dumps(step,ensure_ascii=False,sort_keys=True):observed.output}}))
        assert created.status=='completed',created
        ident=created.output['id'];store.confirm_start_receipt(ident,store.get(ident)['expires_at'])
        engine=LiveTrackerEngine(store,registry=worker,runtime_store=runtime.store,model_client=model,deliver=lambda *args:True)
        engine.check_once(*store.claim(1))
        saved=store.get(ident)
        assert saved['last_outcome']=='no_match' and saved['consecutive_errors']==0,{'problem':saved.get('last_problem'),'packets':model.packets}
        assert model.repairs==1 and saved['probe'][0]['arguments']['ready_selector']=='h3'
        assert saved['probe'][0]['arguments']['selector']=='#reading' and saved['probe'][0]['arguments']['ready_text']==label
        assert saved['comparison_snapshot']['plan_binding']==snapshot_plan_binding(saved['probe'],saved['probe_contexts'])
        value['reading']=37
        engine.check_once(*store.claim(1,now=utcnow()+timedelta(seconds=10)))
        saved=store.get(ident)
        assert saved['last_outcome']=='matched' and saved['state']=='active' and saved['consecutive_errors']==0,saved
        assert model.repairs==1 and model.history==[False,True]
        notice=store.claim_notice()
        assert notice and notice['kind']=='matched' and '37' in notice['text'],notice
        assert value['loads']==3,'Source was repeatedly reopened during repair'
    finally:
        creator.run_cleanup_hooks();worker.run_cleanup_hooks()
        source.shutdown();source.server_close()

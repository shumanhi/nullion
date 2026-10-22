import json,os,time
from pathlib import Path
import pytest
from test_scheduled_report_completion import setup_runtime,text_response,tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E only')


def test_timed_out_planner_falls_back_to_real_chat_and_delivers_complete_html(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import create_plugin_tool_registry
    runtime=setup_runtime(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED','1');monkeypatch.setenv('NULLION_MULTI_AGENT_ENABLED','1')
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1');chat_store._store=None
    registry=create_plugin_tool_registry(workspace_root=tmp_path,allowed_roots=[tmp_path])
    samples=[];paths=[]
    class Model:
        planner_calls=0; deadlines=[]; feedback=[]
        def create(self,**kw):
            self.feedback.append(kw['messages'][-1])
            system=str(kw.get('system') or '');names={t['name'] for t in kw.get('tools',[])}
            if system.startswith('You are a language-neutral structured planner'):
                self.planner_calls+=1;self.deadlines.append(kw.get('timeout'))
                if not kw.get('timeout'):time.sleep(1.5)
                raise TimeoutError('Simulated previously unbounded planner provider')
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text_response(json.dumps({'valid':True,'requested_tool_names':['file_write'],
                'required_tool_names':['file_write'],'requested_artifact_extensions':['.html'],'confidence':1}))
            if 'file_write' not in names:return tool_response('request_tool_scope',{'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':['.html']})
            from nullion.artifacts import artifact_path_for_generated_workspace_file
            if len(paths)<self.planner_calls:
                paths.append(artifact_path_for_generated_workspace_file(principal_id='web_user',suffix='.html',stem='report-'+str(self.planner_calls)))
            path=paths[self.planner_calls-1]
            if not path.exists():
                return tool_response('file_write',{'path':str(path),'content':'<!doctype html><html><body><h1>OK</h1><p>Complete report.</p></body></html>'})
            return text_response('Report attached.\nMEDIA:'+str(path))
    model=Model()
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        for i,prompt in enumerate(('Create an HTML file containing OK.','OKと記載したHTMLファイルを作成。','Crea un archivo HTML que contenga OK.')):
            start=time.perf_counter()
            response=client.post('/api/chat',json={'text':'/planner '+prompt,'conversation_id':'web:planner-deadline-'+str(i)})
            samples.append(time.perf_counter()-start)
            assert response.status_code==200,response.text
            payload=response.json()
            if not payload.get('artifacts'):(tmp_path/'model-feedback.json').write_text(json.dumps(model.feedback,ensure_ascii=False))
            assert payload.get('artifacts'),(payload,model.feedback[-3:])
            descriptor=payload['artifacts'][0]
            download=client.get(descriptor['url']);assert download.status_code==200
            assert b'<h1>OK</h1>' in download.content
    assert model.planner_calls==3 and model.deadlines==[30.0]*3
    assert sum(samples)/len(samples)<3 and max(samples)<4.5,samples

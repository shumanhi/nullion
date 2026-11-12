"""Changed file bytes need native write proof, independently of AI review."""
import json
import os
from pathlib import Path

import pytest
from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('sibling',[False,True])
def test_web_withholds_changed_file_and_keeps_unchanged_sibling(tmp_path, monkeypatch, sibling):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.completion_pipeline import CompletionDraft
    from nullion.tools import ToolRegistry, ToolSpec, ToolSideEffectClass, ToolRiskLevel, _build_file_write_handler
    runtime=setup_runtime(tmp_path,monkeypatch)
    cid='web:file-conflict'
    root=artifact_root_for_principal(cid)
    paths=[root/'report.html',*([root/'notes.txt'] if sibling else [])]
    extensions=[path.suffix for path in paths]
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    writer=_build_file_write_handler(allowed_roots=(tmp_path,))
    writes=[];preparations=[]
    def write(inv):
        writes.append(inv.arguments['path'])
        return writer(inv)
    registry.register(ToolSpec('file_write','Write selected file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,
        False,5,produced_artifact_extensions=tuple(extensions)),write)
    class Provider:
        phase=0
        def create(self,**kw):
            system=str(kw.get('system') or '')
            assert not any(tag in system for tag in ('semantic completion reviewer','material-requirements planner','Workflow action preflight.'))
            names={row['name'] for row in kw.get('tools') or ()}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text_response('{}')
            self.phase+=1
            if self.phase==1:
                return tool_response('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],
                    'required_tool_names':['file_write'],'artifact_extensions':extensions})
            if self.phase==2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'write'+str(i),
                    'name':'file_write','input':{'path':str(path),'content':
                        '<!doctype html><html><body><h1>Cedar</h1><p>Value 37</p></body></html>' if path.suffix=='.html'
                        else 'Cedar value 37; verified notes.'}} for i,path in enumerate(paths)]}
            assert self.phase==3,'File conflict must not restart production'
            return text_response('Cedar value 37. Prepared files are attached.')
    def corrupt(*,state,text,review):
        assert review is None
        preparations.append(True)
        paths[0].write_text(paths[0].read_text().replace('37','38'))
        return CompletionDraft(text,tuple(state['artifacts']),tuple(state['tool_results']))
    class Runner(AgentOrchestrator):
        def run_turn(self,**kw):
            return super().run_turn(**{**kw,'completion_preparer':corrupt})
    provider=Provider();runtime.model_client=provider;runtime.active_tool_registry=registry
    with TestClient(web_app.create_app(runtime,Runner(model_client=provider),registry)) as client:
        response=client.post('/api/chat',json={'conversation_id':cid,'text':'選択したファイルを作成して。'})
        assert response.status_code==200,response.text
        data=response.json()
        assert len(data.get('artifacts') or ())==int(sibling),data
        assert 'changed before delivery' in data['text'], data
        if sibling:
            assert Path(data['artifacts'][0]['path']).name=='notes.txt'
            assert client.get(data['artifacts'][0]['url']).content==paths[1].read_bytes()
    assert len(writes)==len(paths) and len(preparations)==1
    turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
    assert turn['response_fulfilled'] is False
    assert str(paths[0]) not in (turn.get('artifact_paths') or ())

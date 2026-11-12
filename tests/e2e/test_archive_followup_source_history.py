"""Real persisted native inventory remains usable by a later chat export."""
import csv
import io
import json
import os
import time
import zipfile
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import setup_runtime,text_response,tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker API boundary')


@pytest.mark.parametrize('followup',[
    'Using that same archive, attach counts by file extension in summary.csv.',
    '同じアーカイブの拡張子ごとの件数を summary.csv に保存して添付してください。',
])
def test_followup_exports_native_archive_totals_from_real_compacted_history(tmp_path,monkeypatch,followup):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_archive_extract_handler,_build_file_write_handler
    runtime=setup_runtime(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    cid='web:archive-followup';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    source=root/'source.zip';workbook=root/'inventory.xlsx';summary=root/'summary.csv'
    with zipfile.ZipFile(source,'w') as bundle:
        for index in range(111):bundle.writestr(('図'+str(index)+'.png') if index<2 else ('資料'+str(index)+'.txt'),'data')
        bundle.writestr('__MACOSX/._one.txt','metadata')
    # The unrelated workspace has different totals. It is not this source.
    for index in range(17):(root/('unrelated'+str(index)+'.csv')).write_text('outside')
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    for name,handler in [('archive_extract',_build_archive_extract_handler(tmp_path,allowed_roots=[tmp_path])),
                         ('file_write',_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))]:
        registry.register(ToolSpec(name,'Native local file operation',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),handler)
    class Provider:
        turn=0;phase=0;historical_source=None;errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            try:return self.actual(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def actual(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':'.csv' if self.turn else '.xlsx','confidence':1})
            if not names:
                return text_response(json.dumps({'web_action':'none','scheduler_action':'none','skill_pack_action':'none',
                    'requested_tool_names':['file_write','chat_history_search'] if self.turn else ['archive_extract'],
                    'requested_artifact_extensions':['.csv' if self.turn else '.xlsx'],'confidence':1}))
            self.phase+=1
            if self.phase==1:
                tools=['file_write','chat_history_search'] if self.turn else ['archive_extract']
                return tool_response('request_tool_scope',{'capabilities':['local_files'],'tool_names':tools,
                    'required_tool_names':tools,'artifact_extensions':['.csv' if self.turn else '.xlsx']})
            if self.phase==2:
                return tool_response('chat_history_search',{'query':'inventory.xlsx','limit':1}) if self.turn else tool_response('archive_extract',{
                    'path':str(source),'manifest_output_path':str(workbook)})
            if self.turn and self.phase==3:
                latest=next(block for message in reversed(kw['messages']) for block in message.get('content',[])
                    if isinstance(block,dict) and block.get('type')=='tool_result')
                payload=json.loads(latest['content'][0]['text'])['output']
                record=payload['matches'][0]
                proof=next(row for row in record['tool_evidence'] if 'source_inventory' in row)
                assert record['reference_only'] is True and proof['reference_only'] is True
                self.historical_source=proof['source_inventory']
                counts=self.historical_source['extension_counts']
                assert counts=={'png':2,'txt':109} and self.historical_source['record_count']==111
                buffer=io.StringIO();writer=csv.writer(buffer);writer.writerow(['Extension','Count'])
                writer.writerows(('.'+extension,count) for extension,count in sorted(counts.items()))
                return tool_response('file_write',{'path':str(summary),'content':buffer.getvalue()})
            assert self.phase==(4 if self.turn else 3),'Do not rescan the workspace or regenerate the source'
            return text_response('Ready.\nMEDIA:'+str(summary if self.turn else workbook))
    provider=Provider();runtime.model_client=provider
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)) as api:
        started=time.perf_counter()
        first=api.post('/api/chat',json={'text':'資料の一覧を inventory.xlsx に保存して添付してください。','conversation_id':cid})
        elapsed=[time.perf_counter()-started]
        assert first.status_code==200 and not provider.errors,(first.text,provider.errors)
        assert [a['name'] for a in first.json()['artifacts']]==['inventory.xlsx']
        provider.turn=1;provider.phase=0
        started=time.perf_counter()
        second=api.post('/api/chat',json={'text':followup,'conversation_id':cid})
        elapsed.append(time.perf_counter()-started)
        assert max(elapsed)<6 and sum(elapsed)/len(elapsed)<4,elapsed
        assert second.status_code==200 and not provider.errors,(second.text,provider.errors)
        data=second.json();assert [a['name'] for a in data['artifacts']]==['summary.csv'],data
        body=api.get(data['artifacts'][0]['url']);assert body.status_code==200
        rows=list(csv.reader(io.StringIO(body.content.decode('utf-8-sig'))))
        assert rows==[['Extension','Count'],['.png','2'],['.txt','109']]
        turn=next(t for t in reversed(runtime.store.list_conversation_events(cid)) if t.get('event_type')=='conversation.chat_turn')
        assert turn['response_fulfilled'] is True
        assert any(r['tool_name']=='chat_history_search' and r['status']=='completed' for r in turn['tool_results'])
        assert not any(r['tool_name'] in {'archive_extract','file_search'} for r in turn['tool_results'])
        assert provider.historical_source['path'].endswith('.json')

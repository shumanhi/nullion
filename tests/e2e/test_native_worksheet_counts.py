"""Native row-count repair creates one actual downloadable multi-sheet book."""
import io
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.test_spreadsheet_row_scope import sheets

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('prompt',['Deliver four rows in a data sheet and fourteen notes in a separate sheet.',
    '比較用の４行と、別シートに１４行の注記を含むブックを作成して添付してください。'])
def test_native_count_correction_delivers_all_rows_once(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from openpyxl import load_workbook
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_spreadsheet_create_handler
    runtime=setup_runtime(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    cid='web:worksheet-count';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    output=root/'比較.xlsx';results=[]
    native=_build_spreadsheet_create_handler(tmp_path,allowed_roots=[tmp_path])
    def create(inv):
        result=native(inv);results.append(result);return result
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('spreadsheet_create','Create multi-sheet workbook with declared row counts',
        ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,20),create)
    class Provider:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw)
            if protocol is not None:return protocol
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['spreadsheet_create'],'artifact_extensions':['.xlsx']})
            if self.phase==2:return tool_response('spreadsheet_create',{'output_path':str(output),'sheets':sheets(),'expected_rows':4})
            if self.phase==3:
                assert results[-1].output['validation']['scope']=='workbook_total' and not output.exists()
                return tool_response('spreadsheet_create',{'output_path':str(output),'sheets':sheets()})
            assert self.phase==4,'A count correction must not restart creation after success'
            return text_response('✓\nMEDIA:'+str(output))
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=Provider()),registry=registry)) as api:
        response=api.post('/api/chat',json={'text':prompt,'conversation_id':cid})
        assert response.status_code==200,response.text
        items=response.json().get('artifacts',[]);assert len(items)==1,response.json()
        downloaded=api.get(items[0]['url']);assert downloaded.status_code==200
        book=load_workbook(io.BytesIO(downloaded.content),read_only=True)
        assert book['比較'].max_row==5 and book['Notes'].max_row==15
        assert book['比較']['B5'].value==3 and book['Notes']['A15'].value=='Assumption 13'
        book.close()
        assert [r.status for r in results]==['failed','completed']
        saved=next(row for row in reversed(runtime.store.list_conversation_events(cid))
            if row.get('event_type')=='conversation.chat_turn')
        completed=[r for r in saved['tool_results'] if r['tool_name']=='spreadsheet_create' and r['status']=='completed']
        assert len(completed)==1 and completed[0]['output']['path']==str(output),saved

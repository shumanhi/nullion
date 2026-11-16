"""A real local raster renderer feeds the final workbook without leaking its PNG."""
import io
import json
import os
import shlex
import sys
import time
import zipfile

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('mode',['off','on'])
def test_declared_supporting_png_is_reused_and_only_workbook_is_delivered(tmp_path,monkeypatch,mode,record_property):
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_terminal_exec_handler,_build_spreadsheet_create_handler
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE',mode)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('local',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    native_calls=[]
    for name,builder in [('terminal_exec',_build_terminal_exec_handler),('spreadsheet_create',_build_spreadsheet_create_handler)]:
        native=builder(tmp_path,allowed_roots=[tmp_path])
        def run(invocation,handler=native):
            result=handler(invocation);native_calls.append(result);return result
        registry.register(ToolSpec(name,'Create the selected local output',ToolRiskLevel.LOW,
            ToolSideEffectClass.WRITE,False,20),run)
    class Model:
        phase=0
        errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            try:return self.respond(**kw)
            except Exception as error:self.errors.append(repr(error));raise
        def respond(self,**kw):
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files','local_shell'],
                'tool_names':['terminal_exec','spreadsheet_create'],'required_tool_names':['terminal_exec','spreadsheet_create'],
                'artifact_extensions':['.xlsx']})
            if self.phase==2:
                if mode=='on':
                    code="raster=call_tool('terminal_exec', "+repr(self.render)+")\n"
                    code+="assert raster['status']=='completed', 'render failed'\n"
                    code+="sheet=call_tool('spreadsheet_create', "+repr(self.sheet)+")\nsheet['output']"
                    return call('code_mode',{'tool_names':['terminal_exec','spreadsheet_create'],'code':code})
                return call('terminal_exec',self.render)
            if mode=='off' and self.phase==3:
                assert 'generated_outputs' in json.dumps(kw['messages'])
                return call('spreadsheet_create',self.sheet)
            assert native_calls[-2].status=='completed',native_calls[-2]
            assert native_calls[-2].output['generated_outputs'][0]['role']=='intermediate'
            return text('Completed the workbook.\nMEDIA:'+self.sheet['output_path'])
    samples=[]
    model=Model()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['画像を埋め込んだ result.xlsx だけ添付してください。',
                'Adjunta solo result.xlsx con una imagen incrustada.',
                'Joignez uniquement result.xlsx avec une image intégrée.']):
            cid='web:local-support-'+str(index)
            root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
            png=root/f'support-{index}.png';xlsx=root/f'result-{index}.xlsx'
            script="from PIL import Image;Image.new('RGB',(80,60),'navy').save("+repr(str(png))+")"
            model.phase=0
            model.render={'command':shlex.quote(sys.executable)+' -c '+shlex.quote(script),'output_paths':[str(png)]}
            model.sheet={'output_path':str(xlsx),'columns':['Value'],'rows':[[37]],'image_paths':[str(png)]}
            started=time.monotonic();response=client.post('/api/chat',json={'conversation_id':cid,'text':prompt.replace('result.xlsx',xlsx.name)})
            samples.append(time.monotonic()-started)
            assert not model.errors,model.errors
            assert response.status_code==200,response.text
            artifacts=response.json().get('artifacts',[])
            assert len(artifacts)==1 and artifacts[0]['name']==xlsx.name,response.text
            data=client.get(artifacts[0]['url']).content
            with zipfile.ZipFile(io.BytesIO(data)) as package:
                images=[n for n in package.namelist() if n.startswith('xl/media/')]
                assert len(images)==1 and package.read(images[0])==png.read_bytes()
    assert len(native_calls)==6 and all(r.status=='completed' for r in native_calls)
    assert sum(samples)/len(samples)<5 and max(samples)<8,samples
    record_property('mean_supporting_image_seconds',sum(samples)/len(samples))

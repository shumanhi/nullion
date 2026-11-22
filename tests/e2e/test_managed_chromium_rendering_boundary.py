"""Real API/SQLite rendering with installer-managed Chromium and no system browser."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker browser boundary')


@pytest.mark.parametrize('renderer',['text_pdf','designed_pdf','image'])
def test_api_delivers_real_artifact_when_only_managed_chromium_is_installed(tmp_path,monkeypatch,record_property,renderer):
    from fastapi.testclient import TestClient
    from PIL import Image
    from pypdf import PdfReader
    from playwright.sync_api import sync_playwright,BrowserType
    from nullion import tools,image_render
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','off')
    monkeypatch.delenv('NULLION_CHROMIUM_EXECUTABLE',raising=False)
    real_which=tools.shutil.which
    browser_commands={'google-chrome','chromium','chromium-browser','chrome'}
    monkeypatch.setattr(tools.shutil,'which',lambda name,*args,**kwargs:
        None if name in browser_commands else real_which(name,*args,**kwargs))
    # Exercise actual discovery with browser commands hidden. The QA image's
    # PATH symlink is never selected, and the resolver/renderers stay real.
    assert tools._pdf_chromium_executable() is None
    with sync_playwright() as playwright:
        managed=playwright.chromium.executable_path
    assert Path(managed).is_file() and managed!='/usr/local/bin/chromium'
    launches=[];real_launch=BrowserType.launch
    def launch(self,*args,**kwargs):
        launches.append({'name':self.name,**kwargs})
        assert kwargs.get('executable_path')==managed and kwargs.get('headless') is True
        return real_launch(self,*args,**kwargs)
    monkeypatch.setattr(BrowserType,'launch',launch)
    before={name:os.environ.get(name) for name in ('NULLION_CHROMIUM_EXECUTABLE','PLAYWRIGHT_BROWSERS_PATH',
        'NULLION_BROWSER_BACKEND','NULLION_BROWSER_CDP_URL','NULLION_BROWSER_PREFERRED')}
    cid='web:managed-render-'+renderer;root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    suffix='.png' if renderer=='image' else '.pdf';target=root/('managed-'+renderer+suffix)
    title='Managed Chromium report'
    content='Cedar trip window: 2031-04-09 to 2031-04-10. Native source count: 37.'
    if renderer=='image':
        name='image_render';native=image_render.build_handler(tmp_path,[tmp_path]);arguments={
            'card_pages':[{'title':title,'subtitle':'Installer-managed rendering',
                'cards':[{'heading':'Cedar trip','text':content}]}],'width':800,'pixel_ratio':1}
    else:
        name='pdf_create';native=tools._build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path])
        arguments={'title':title,'page_size':'a4','pagination':'flow'}
        if renderer=='text_pdf':arguments['text_pages']=['# Cedar trip\n\n'+content]
        else:arguments['html_pages']=['<main><h1>Cedar trip</h1><p>'+content+'</p><p>Managed browser artifact receipt verified.</p></main>']
    receipts=[]
    def handler(invocation):
        result=native(invocation);receipts.append(result);return result
    registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(tools.ToolSpec(name,'Render requested local artifact',tools.ToolRiskLevel.LOW,
        tools.ToolSideEffectClass.WRITE,False,30,produced_artifact_extensions=(suffix,)),handler)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            names={entry['name'] for entry in kwargs.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':suffix,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':[name],
                'required_tool_names':[name],'artifact_extensions':[suffix]})
            if self.phase==2:return call(name,{'output_path':str(target),**arguments})
            assert self.phase==3
            return text('完成しました。\nMEDIA:'+str(target))
    model=Model()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        response=client.post('/api/chat',json={'text':'資料を '+suffix+' でください。','conversation_id':cid})
        assert response.status_code==200 and len(response.json()['artifacts'])==1,response.text
        advertised=response.json()['artifacts'][0]
        served=client.get(advertised['url']);assert served.status_code==200
        data=served.content;assert data==target.read_bytes()
    assert len(receipts)==1 and receipts[0].status=='completed',receipts
    assert len(launches)==1 and model.phase==3,launches
    assert before=={name:os.environ.get(name) for name in before}
    turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
    assert turn['response_fulfilled'] is True and any(row['tool_name']==name and row['status']=='completed' for row in turn['tool_results'])
    evidence=Path('/outputs/managed-chromium') if Path('/outputs').is_dir() else tmp_path/'managed-chromium'
    evidence.mkdir(parents=True,exist_ok=True)
    output=evidence/target.name;output.write_bytes(data)
    if suffix=='.pdf':
        pages=PdfReader(io.BytesIO(data)).pages
        assert len(pages)==1 and '2031-04-09' in pages[0].extract_text()
        assert receipts[0].output['text_layer'] is True
        assert receipts[0].output['pagination_receipt']['scale']==1
        assert receipts[0].output['visual_preview_available'] is True
        subprocess.run(['/usr/bin/pdftoppm','-scale-to','1400','-png',str(output),str(evidence/renderer)],check=True,timeout=30)
    else:
        with Image.open(io.BytesIO(data)) as picture:
            assert picture.width==800 and picture.height>150 and picture.getbbox()
        assert receipts[0].output['image_sha256']==hashlib.sha256(data).hexdigest()
    (evidence/(renderer+'-receipt.json')).write_text(json.dumps({'native_receipt':receipts[0].output,
        'launches':launches,'artifact_sha256':hashlib.sha256(data).hexdigest(),'native_turn_id':turn['turn_id']},indent=2))
    record_property('managed_renderer_path',managed)

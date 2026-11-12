"""Faithful SVG conversion at real Chromium, PDF, HTTP, and Office boundaries."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io
import json
import os
from pathlib import Path
from zipfile import ZipFile

import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Real rendering in Docker only')

SVG='<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100" viewBox="0 0 200 100"><rect width="100" height="50" fill="#ff0000"/><rect x="100" width="100" height="50" fill="#0000ff"/><rect y="50" width="100" height="50" fill="#00ff00"/><rect x="100" y="50" width="100" height="50" fill="#ffff00"/></svg>'


def pixels(data):
    from PIL import Image
    image=Image.open(io.BytesIO(data)).convert('RGB');w,h=image.size
    actual=[image.getpixel((int(w*x),int(h*y))) for x,y in ((.25,.25),(.75,.25),(.25,.75),(.75,.75))]
    assert actual==[(255,0,0),(0,0,255),(0,255,0),(255,255,0)],actual


def test_svg_conversion_preserves_pixels_and_invalidates_only_its_own_cache(tmp_path):
    import hashlib
    from nullion.tools import _embeddable_image_path
    from PIL import Image
    source=tmp_path/'chart.svg';source.write_text(SVG)
    companion=source.with_suffix('.png');Image.new('RGB',(200,100),'black').save(companion)
    before=companion.read_bytes()
    first=_embeddable_image_path(source)
    assert first!=source and first!=companion
    pixels(first.read_bytes());stamp=first.stat().st_mtime_ns
    assert _embeddable_image_path(source)==first and first.stat().st_mtime_ns==stamp
    assert companion.read_bytes()==before
    source.write_text(SVG.replace('#ff0000','#ff00ff'))
    changed=_embeddable_image_path(source)
    assert changed!=first and changed.is_file() and companion.read_bytes()==before
    source.write_text(SVG.replace('#ff0000','#00ffff'))
    digest=hashlib.sha256(b'svg-chromium-v1:'+source.read_bytes()).hexdigest()[:20]
    cache=source.with_name(f'.{source.stem}.rendered-{digest}.png')
    cache.symlink_to(companion)
    assert _embeddable_image_path(source)==source
    assert companion.read_bytes()==before and cache.is_symlink()
    assert not list(tmp_path.glob('*.tmp.png'))


def test_svg_conversion_is_inert_and_does_not_fetch_external_resources(tmp_path):
    import threading
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    from nullion.tools import _render_svg_raster
    hits=[]
    class External(BaseHTTPRequestHandler):
        def do_GET(self):hits.append(self.path);self.send_response(200);self.end_headers()
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),External)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        target=f'http://localhost:{server.server_port}/private'
        source=tmp_path/'inert.svg';source.write_text(SVG.replace('</svg>',f'<script>fetch("{target}")</script></svg>'))
        pixels(_render_svg_raster(source).read_bytes())
        for index,extra in enumerate([f'<image href="{target}"/>','<image href="file:///private/secret.png"/>',
            f'<style>rect{{fill:url({target})}}</style>']):
            bad=tmp_path/(str(index)+'.svg');bad.write_text(SVG.replace('</svg>',extra+'</svg>'))
            assert _render_svg_raster(bad) is None
        assert not hits
    finally:
        server.shutdown();server.server_close()


@pytest.mark.parametrize('family',['pdf','document','presentation','spreadsheet'])
def test_svg_chart_reaches_the_real_web_download_with_actual_pixels(tmp_path,monkeypatch,family):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from pypdf import PdfReader
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler,_build_pdf_create_handler,_build_document_create_handler,_build_presentation_create_handler,_build_spreadsheet_create_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    with sync_playwright() as playwright:monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',playwright.chromium.executable_path)
    conversation='web:svg-'+family;root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    create_name,extension,builder,create_arguments={
        'pdf':('pdf_create','.pdf',_build_pdf_create_handler,{'html_pages':['<h1>Four-color chart</h1><p>All four quadrants must be visible.</p>']}),
        'document':('document_create','.docx',_build_document_create_handler,{'paragraphs':['All four quadrants must be visible.']}),
        'presentation':('presentation_create','.pptx',_build_presentation_create_handler,{'slides':[{'title':'Four-color chart','bullets':['All four quadrants must be visible.']}]}),
        'spreadsheet':('spreadsheet_create','.xlsx',_build_spreadsheet_create_handler,{'columns':['Record'],'rows':[['All four quadrants must be visible.']]}),
    }[family]
    source=root/'chart.svg';final=root/('chart'+extension)
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    for name,handler in [('file_write',_build_file_write_handler(tmp_path,allowed_roots=[tmp_path])),
            (create_name,builder(tmp_path,allowed_roots=[tmp_path]))]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),handler)
    class Provider:
        phase=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={r['name'] for r in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver the four-color chart PDF','requirement_kind':'deliverable','evidence_subject_id':'chart',
                        'eligible_evidence_tool_names':[create_name]}]}))
            if system.startswith('You are a semantic completion reviewer'):
                value=json.loads(kw['messages'][0]['content'][0]['text'])
                ids=[r['invocation_id'] for r in value['tool_evidence'] if r.get('output',{}).get('result_kind')=='artifact_receipt']
                return text(json.dumps({'disposition':'complete','unresolved_requirements':[],'retry_tool_names':[],
                    'all_material_requirements_enumerated':True,'material_requirements':[{**row,'satisfied':True,'draft_covered':True,
                        'evidence_invocation_ids':ids} for row in value['material_requirement_contract']]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':extension,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write',create_name],
                'required_tool_names':[create_name],'artifact_extensions':[extension],'embedded_media_artifact_extensions':[extension]})
            if self.phase==2:return tool('file_write',{'path':str(source),'content':SVG})
            if self.phase==3:return tool(create_name,{'title':'Four-color chart',**create_arguments,
                'image_paths':[str(source)],'output_path':str(final)})
            assert self.phase==4,'Rendering must not require a generated placeholder or retry'
            return text('Prepared the chart.\nMEDIA:'+str(final))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':'Create one '+extension+' file containing the complete red, blue, green and yellow SVG chart.','conversation_id':conversation})
        assert response.status_code==200,response.text
        data=response.json();assert len(data['artifacts'])==1,data
        download=client.get(data['artifacts'][0]['url']);assert download.status_code==200
        if family=='pdf':
            reader=PdfReader(io.BytesIO(download.content));images=[image for page in reader.pages for image in page.images]
            assert images,'The real SVG must be embedded as an image'
            pixels(images[0].data)
            assert 'Generated visual' not in ''.join(p.extract_text() or '' for p in reader.pages)
        else:
            with ZipFile(io.BytesIO(download.content)) as office:
                images=[name for name in office.namelist() if '/media/' in name and name.endswith('.png')]
                assert images,'The downloaded Office file must embed the actual rendered artwork'
                pixels(office.read(images[0]))
                assert all(b'Generated visual' not in office.read(name) for name in office.namelist() if name.endswith('.xml'))
        turns=[e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type')=='conversation.chat_turn']
        assert sum(r['tool_name']==create_name and r['status']=='completed' for r in turns[-1]['tool_results'])==1

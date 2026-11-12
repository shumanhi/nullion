"""Inline SVG reaches actual PDF image bytes without a model rebuilding it."""
import io
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

from tests.test_inline_svg_media import SVG, illustration_html
from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.e2e.test_svg_raster_delivery_workflow import pixels
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Native Chromium/PDF boundary in Docker')


@pytest.mark.parametrize('svg_form',['inline','base64','percent'])
@pytest.mark.parametrize('prompt,plain_sibling',[
    ('Create and attach one PDF from the supplied inline illustration.',False),
    ('インラインのイラストを含む１ページのPDFを作成して添付してください。',False),
    ('Attach an illustrated poster PDF and a separate plain two-page memo PDF.',True),
    ('イラスト付きのPDFと、別の２ページの文章のみのPDFを添付してください。',True)])
def test_inline_svg_delivers_one_pdf_with_exact_illustration_pixels(tmp_path,monkeypatch,prompt,plain_sibling,svg_form):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from pypdf import PdfReader
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_pdf_create_handler
    runtime=setup_runtime(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    with sync_playwright() as browser:monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    cid='web:inline-vector';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    output=root/'illustration.pdf';native=_build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path]);receipts=[];contexts=[]
    def create(inv):
        contexts.append(inv.flow_context)
        result=native(inv);receipts.append(result);return result
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('pdf_create','Create PDF from text, images, or HTML',
        ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),create)
    class Provider:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw)
            if protocol is not None:return protocol
            names={tool['name'] for tool in kw.get('tools',[])}
            if 'select_artifact_delivery' in names:
                import json
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                candidates=json.loads(raw)
                assert plain_sibling and len(candidates)==2,candidates
                return tool_response('select_artifact_delivery',{'status':'selected',
                    'selected_ids':[candidate['candidate_id'] for candidate in candidates],
                    'reason':'Both distinct completed PDFs are explicitly requested.'})
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format',{'extension':'.pdf','confidence':1})
            if not names:return text_response('{}')
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['local_files'],
                    'tool_names':['pdf_create'],'required_tool_names':['pdf_create'],
                    'artifact_extensions':['.pdf'],'embedded_media_artifact_extensions':['.pdf']})
            if self.phase==2:return tool_response('pdf_create',{'output_path':str(output),
                'html_pages':['<h1>INLINE VECTOR 53</h1>'+illustration_html(svg_form)], 'expected_page_count':1})
            if plain_sibling and self.phase==3:return tool_response('pdf_create',{
                'output_path':'memo.pdf','html_pages':['<h1>Memo page one</h1>','<h1>Memo page two</h1>'],
                'expected_page_count':2})
            assert self.phase==(4 if plain_sibling else 3),'No illustration regeneration or extra tool is needed'
            memo_path=receipts[-1].output['path'] if plain_sibling else None
            return text_response('Ready.\nMEDIA:'+str(output)+ ('\nMEDIA:'+memo_path if memo_path else ''))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as api:
        response=api.post('/api/chat',json={'text':prompt,'conversation_id':cid}).json()
        assert contexts and contexts[0].get('required_embedded_media_extensions') == ('.pdf',), (
            contexts, [event.get('tool_results') for event in runtime.store.list_conversation_events(cid)
                       if event.get('tool_results')])
        assert len(response.get('artifacts',[]))==(2 if plain_sibling else 1),(response,receipts)
        artifact=next(a for a in response['artifacts'] if a['name']=='illustration.pdf')
        download=api.get(artifact['url']);assert download.status_code==200
        document=PdfReader(io.BytesIO(download.content));assert len(document.pages)==1
        assert 'INLINE VECTOR 53' in document.pages[0].extract_text()
        assert len(document.pages[0].images)==1
        pixels(document.pages[0].images[0].data)
        assert len(receipts)==(2 if plain_sibling else 1) and all(r.status=='completed' for r in receipts),receipts
        assert receipts[0].output['inspection']['embedded_image_count']==1
        assert receipts[0].output['embedded_html_images'][0]['kind']==('rasterized_inline_svg'
            if svg_form=='inline' else 'rasterized_svg_data_image')
        assert not receipts[0].output['source_image_paths']
        turn=next(e for e in reversed(runtime.store.list_conversation_events(cid)) if e.get('event_type')=='conversation.chat_turn')
        assert turn['response_fulfilled'] is True,turn
        assert [r['tool_name'] for r in turn['tool_results'] if r['tool_name']!='request_tool_scope']==['pdf_create']*(2 if plain_sibling else 1)
        if plain_sibling:
            memo=next(a for a in response['artifacts'] if a['name']=='memo.pdf')
            pages=PdfReader(io.BytesIO(api.get(memo['url']).content)).pages
            assert len(pages)==2 and not any(page.images for page in pages)


@pytest.mark.parametrize('form',['inline','base64','percent'])
def test_inline_svg_external_dependencies_are_not_fetched_or_replaced(tmp_path,monkeypatch,form):
    from nullion.tools import ToolInvocation,_build_pdf_create_handler
    hits=[]
    class External(BaseHTTPRequestHandler):
        def do_GET(self):hits.append(self.path);self.send_response(200);self.end_headers()
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),External);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        output=tmp_path/'bad.pdf';bad=SVG.replace('</svg>',f'<image href="http://127.0.0.1:{server.server_port}/secret"/></svg>')
        if form!='inline':
            from base64 import b64encode
            from urllib.parse import quote
            bad=bad.replace('<svg ','<svg xmlns="http://www.w3.org/2000/svg" ')
            source=('data:image/svg+xml;base64,'+b64encode(bad.encode()).decode()
                    if form=='base64' else 'data:image/svg+xml,'+quote(bad,safe=''))
            bad='<img src="'+source+'">'
        result=_build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False)(
            ToolInvocation('bad','pdf_create','web:bad',{'output_path':str(output),'html_pages':[bad]},
                flow_context={'required_embedded_media_extensions':['.pdf']}))
        assert result.status=='failed' and result.output['reason']=='inline_svg_render_failed',result
        assert not output.exists() and not hits
    finally:server.shutdown();server.server_close();thread.join(timeout=3)


@pytest.mark.parametrize('namespace',['',' xmlns="http://www.w3.org/2000/svg"'])
def test_inline_svg_preserves_paint_from_the_containing_document(tmp_path,monkeypatch,namespace):
    from base64 import b64decode
    from xml.etree import ElementTree as ET
    from playwright.sync_api import sync_playwright
    from nullion.tools import _inline_svg_media_for_pdf
    with sync_playwright() as browser:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    styled=SVG.replace('<svg ', '<svg'+namespace+' ')
    colors=['#ff0000','#0000ff','#00ff00','#ffff00']
    for index,color in enumerate(colors):
        styled=styled.replace('fill="'+color+'"','class="quadrant-'+str(index)+'"')
    css='<style>'+''.join('.quadrant-'+str(i)+'{fill:'+c+'}' for i,c in enumerate(colors))+'</style>'
    converted,evidence=_inline_svg_media_for_pdf(css+styled)
    svg=converted[converted.index('<svg'):]
    root=ET.fromstring(svg)
    image=next(e for e in root.iter() if e.tag.rsplit('}',1)[-1]=='image')
    pixels(b64decode(image.attrib['href'].split(',',1)[1]))
    assert len(evidence)==1


def test_inline_svg_does_not_apply_child_opacity_twice(tmp_path,monkeypatch):
    from base64 import b64decode
    from xml.etree import ElementTree as ET
    from PIL import Image
    from playwright.sync_api import sync_playwright
    from nullion.tools import _inline_svg_media_for_pdf
    with sync_playwright() as browser:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    content='<style>svg > rect {opacity:.5} svg > image {opacity:.1}</style>'+SVG
    converted,_=_inline_svg_media_for_pdf(content)
    root=ET.fromstring(converted[converted.index('<svg'):])
    image=next(e for e in root.iter() if e.tag.rsplit('}',1)[-1]=='image')
    bitmap=Image.open(io.BytesIO(b64decode(image.attrib['href'].split(',',1)[1]))).convert('RGBA')
    assert bitmap.getpixel((25,25))==(255,0,0,128)
    assert 'all: initial !important' in image.attrib['style']


def test_inline_svg_retains_root_styles_without_baking_them_into_pixels(tmp_path,monkeypatch):
    from base64 import b64decode
    from xml.etree import ElementTree as ET
    from PIL import Image
    from playwright.sync_api import sync_playwright
    from nullion.tools import _inline_svg_media_for_pdf
    with sync_playwright() as browser:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    content=SVG.replace('<title>', '<style>svg {opacity:.5} rect {stroke:none}</style><title>')
    converted,_=_inline_svg_media_for_pdf(content)
    root=ET.fromstring(converted)
    image=next(e for e in root.iter() if e.tag.rsplit('}',1)[-1]=='image')
    bitmap=Image.open(io.BytesIO(b64decode(image.attrib['href'].split(',',1)[1]))).convert('RGBA')
    assert bitmap.getpixel((25,25))==(255,0,0,255)
    assert any(e.tag.rsplit('}',1)[-1]=='style' for e in root)


def test_original_flyer_html_creates_one_illustrated_pdf_without_source_replacement(tmp_path,monkeypatch):
    from pathlib import Path
    from pypdf import PdfReader
    from playwright.sync_api import sync_playwright
    from nullion.tools import ToolInvocation,_build_pdf_create_handler
    from nullion.artifact_evidence import inline_svg_fragments
    with sync_playwright() as browser:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    html=(Path(__file__).parents[1]/'fixtures/artifacts/inline_svg_flyer.html').read_text()
    assert len(inline_svg_fragments(html))==1
    output=tmp_path/'poster.pdf'
    result=_build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False)(
        ToolInvocation('original-flyer','pdf_create','web:original-flyer',
            {'output_path':str(output),'html_pages':[html],'expected_page_count':1},
            flow_context={'required_embedded_media_extensions':['.pdf']}))
    assert result.status=='completed',result
    document=PdfReader(output)
    assert len(document.pages)==1
    assert 'Community' in document.pages[0].extract_text()
    assert len(document.pages[0].images)==1
    assert result.output['embedded_html_images'][0]['kind']=='rasterized_inline_svg'
    assert not result.output['source_image_paths']
    assert sorted(p.name for p in tmp_path.iterdir() if p.is_file())==['poster.pdf']


@pytest.mark.parametrize('svg_form',['inline','base64','percent'])
@pytest.mark.parametrize('prompt',['Create and attach one HTML page containing the supplied illustration.',
    'このイラストを含むHTMLファイルを作成して添付してください。'])
def test_inline_svg_html_reaches_real_chat_download_unchanged(tmp_path,monkeypatch,prompt,svg_form):
    from pathlib import Path
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.response_fulfillment_contract import artifact_file_embedded_media_count
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    runtime=setup_runtime(tmp_path,monkeypatch)
    cid='web:inline-html';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    output=root/'illustration.html';body='<html><body>'+illustration_html(svg_form)+'</body></html>'
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    native=_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]);receipts=[]
    def write(inv):
        result=native(inv);receipts.append(result);return result
    registry.register(ToolSpec('file_write','Write structured local HTML',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,30),write)
    class Provider:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw)
            if protocol is not None:return protocol
            names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text_response('{}')
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_write'],'required_tool_names':['file_write'],
                'artifact_extensions':['.html'],'embedded_media_artifact_extensions':['.html']})
            if self.phase==2:return tool_response('file_write',{'path':str(output),'content':body})
            assert self.phase==3,'Valid supplied artwork must not trigger another generation attempt'
            return text_response('Ready.\nMEDIA:'+str(output))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as api:
        response=api.post('/api/chat',json={'text':prompt,'conversation_id':cid}).json()
        assert len(response.get('artifacts',[]))==1,(response,receipts)
        downloaded=api.get(response['artifacts'][0]['url'])
        assert downloaded.status_code==200 and downloaded.content.decode()==body
        assert len(receipts)==1 and receipts[0].status=='completed'
        assert artifact_file_embedded_media_count(output)==1
        turn=next(e for e in reversed(runtime.store.list_conversation_events(cid)) if e.get('event_type')=='conversation.chat_turn')
        assert turn['response_fulfilled'] is True,turn
        with sync_playwright() as browser:
            chromium=browser.chromium.launch()
            try:
                page=chromium.new_page();page.set_content(downloaded.content.decode())
                pixels(page.locator('svg' if svg_form=='inline' else 'img').screenshot())
            finally:chromium.close()


@pytest.mark.parametrize('extra_image',[False,True])
def test_responsive_poster_pdf_keeps_title_and_footer_on_the_visible_page(tmp_path,monkeypatch,extra_image):
    from pathlib import Path
    import pypdfium2 as pdfium
    from playwright.sync_api import sync_playwright
    from nullion.tools import ToolInvocation,_build_pdf_create_handler
    with sync_playwright() as browser:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    content=(Path(__file__).parents[1]/'fixtures/artifacts/responsive_poster.html').read_text()
    image_paths=[]
    if extra_image:
        import re
        from PIL import Image
        content=re.sub(r'<img\s[^>]+>',SVG,content)
        source=tmp_path/'separate.png';Image.new('RGB',(200,100),'purple').save(source)
        image_paths=[str(source)]
    output=tmp_path/'responsive.pdf'
    result=_build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False)(
        ToolInvocation('responsive','pdf_create','web:responsive',
            {'output_path':str(output),'html_pages':[content],'expected_page_count':1,
             'image_paths':image_paths},flow_context={'required_embedded_media_extensions':['.pdf']}))
    assert result.status=='completed',result
    with pdfium.PdfDocument(output) as document:
        assert len(document)==1
        page=document[0];text=page.get_textpage()
        try:
            visible=text.get_text_bounded()
            if extra_image:
                from collections import Counter
                bitmap=page.render(scale=1)
                try:
                    rendered=bitmap.to_pil().convert('RGB')
                    colors=Counter(rendered.getdata())
                    background=rendered.getpixel((round(rendered.width*.15),round(rendered.height*.8)))
                finally:bitmap.close()
                # PDF color conversion may round an RGB channel by one.
                assert all(sum(count for observed,count in colors.items()
                    if all(abs(a-b)<=2 for a,b in zip(observed,color)))>100 for color in [
                    (255,0,0),(0,0,255),(0,255,0),(255,255,0),(128,0,128)]),colors.most_common(8)
                assert all(abs(a-b)<=2 for a,b in zip(background,(16,24,39))),background
        finally:text.close();page.close()
    # PDFium preserves letter spacing as spaces between glyphs.
    compact=''.join(visible.split())
    assert 'COMMUNITY' in compact and 'STAGEFORMATQA' in compact,visible


def test_inline_svg_preserves_local_xlink_definitions_and_pixels(tmp_path,monkeypatch):
    from base64 import b64decode
    from xml.etree import ElementTree as ET
    from playwright.sync_api import sync_playwright
    from nullion.tools import _inline_svg_media_for_pdf
    with sync_playwright() as browser:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    source=SVG.replace('role="img"','xmlns:xlink="http://www.w3.org/1999/xlink" role="img"')
    source=source.replace('<rect width=', '<defs><g id="colors"><rect width=',1)
    source=source.replace('</svg>','</g></defs><use xlink:href="#colors"/></svg>')
    converted,_=_inline_svg_media_for_pdf(source)
    root=ET.fromstring(converted)
    image=next(e for e in root.iter() if e.tag.rsplit('}',1)[-1]=='image')
    pixels(b64decode(image.attrib['href'].split(',',1)[1]))


def test_inline_svg_html_entities_do_not_force_image_regeneration(tmp_path,monkeypatch):
    from base64 import b64decode
    from xml.etree import ElementTree as ET
    from playwright.sync_api import sync_playwright
    from nullion.tools import _inline_svg_media_for_pdf
    with sync_playwright() as browser:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',browser.chromium.executable_path)
    source=SVG.replace('Four colors','R&D&nbsp;&copy;')
    converted,_=_inline_svg_media_for_pdf(source)
    root=ET.fromstring(converted)
    image=next(e for e in root.iter() if e.tag.rsplit('}',1)[-1]=='image')
    pixels(b64decode(image.attrib['href'].split(',',1)[1]))
    assert root.find('{http://www.w3.org/2000/svg}title').text=='R&D\u00a0©'

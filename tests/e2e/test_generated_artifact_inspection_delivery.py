"""Inspecting a generated binary document must not invalidate its producer proof."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import io,json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container real product boundary')

@pytest.mark.parametrize('plain_sibling',[False,True])
@pytest.mark.parametrize('unknown_reference',[None,'pdf','png','csv'])
@pytest.mark.parametrize('prompt',['Create and inspect an illustrated PDF with value 37 and a separate CSV of that value.','Crea y revisa un PDF ilustrado con el valor 37 y un CSV separado del valor.','値37の画像付きPDFと別のCSVを作成し、PDFを確認して両方渡してください。'])
def test_generated_pdf_keeps_media_proof_after_native_read(tmp_path,monkeypatch,record_property,prompt,plain_sibling,unknown_reference):
    from fastapi.testclient import TestClient
    from PIL import Image,ImageDraw
    from pypdf import PdfReader
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    import random
    image=Image.frombytes('RGB',(300,300),random.Random(73).randbytes(300*300*3))
    b=io.BytesIO();image.save(b,format='PNG');body=b.getvalue()
    monkeypatch.setattr(tools,'_fetch_browser_image_binary',lambda url,timeout_seconds:{'source_url':url,'final_url':url,'status_code':200,'content_type':'image/png','data':body})
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');root=artifact_root_for_principal('web:read-after-write');root.mkdir(parents=True,exist_ok=True);pdf=root/'report.pdf';memo=root/'memo.pdf';csv=root/'values.csv';collected=[];writes=[];reads=[]
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from datetime import datetime,UTC
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-source',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='example.org',created_by='fixture-user',created_at=datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);collector=tools._build_browser_image_collect_handler();writer=tools._build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path]);reader=tools._build_file_read_handler(tmp_path,allowed_roots=[tmp_path])
    def collect(inv):r=collector(inv);collected.append(r);return r
    def write(inv):r=writer(inv);writes.append(r);return r
    def read(inv):r=reader(inv);reads.append(r);return r
    for name,effect,fn in [('browser_image_collect',ToolSideEffectClass.WRITE,collect),('pdf_create',ToolSideEffectClass.WRITE,write),('file_read',ToolSideEffectClass.READ,read),('file_write',ToolSideEffectClass.WRITE,tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,30),fn)
    class Provider:
        phase=0;errors=[];reviews=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def tool(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'),'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Deliver the illustrated PDF after inspection','requirement_kind':'deliverable','evidence_subject_id':'pdf','eligible_evidence_tool_names':['pdf_create']},{'requirement':'Deliver the separate CSV','requirement_kind':'deliverable','evidence_subject_id':'csv','eligible_evidence_tool_names':['file_write']},*([{'requirement':'Deliver the separate plain memo PDF','requirement_kind':'deliverable','evidence_subject_id':'memo','eligible_evidence_tool_names':['pdf_create']}] if plain_sibling else [])]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1
                data=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                for r in data['material_requirement_contract']:
                    filename={'csv':'values.csv','memo':'memo.pdf'}.get(r['evidence_subject_id'],'report.pdf')
                    records=[x['invocation_id'] for x in data['tool_evidence'] if x.get('output',{}).get('artifact_record_binding',{}).get('filename')==filename]
                    assert len(records)==1
                    coverage.append({**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':records})
                return text(json.dumps({**fixture_source_assessment(data,conclusion='limited'),'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:
                packet=kw['messages'][0]['content'][0]['text']
                marker='Prepared file candidates: '
                packet=packet.split(marker,1)[1].split('\n',1)[1]
                packet=packet.split('\n')[0]
                candidates=json.loads(packet)
                ids=[r['candidate_id'] for r in candidates if r.get('filename') in {'report.pdf','values.csv',*({'memo.pdf'} if plain_sibling else set())}]
                assert len(ids)==(3 if plain_sibling else 2),candidates
                return tool('select_artifact_delivery',{'status':'selected','selected_ids':ids,'reason':'All explicitly requested final files.'})
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.pdf','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['browser_image_collect','pdf_create','file_read','file_write'],'required_tool_names':['browser_image_collect','pdf_create','file_read','file_write'],'artifact_extensions':['.pdf','.csv'],'embedded_media_artifact_extensions':['.pdf']})
            if self.phase==2:return tool('browser_image_collect',{'image_urls':['https://example.org/illustration.png'],'max_images':1})
            if self.phase==3:
                image_path=collected[0].output['image_paths'][0]
                return {'stop_reason':'tool_use','content':[
                    {'type':'tool_use','id':'pdf','name':'pdf_create','input':{'output_path':str(pdf),'html_pages':['<h1>Verified report</h1><p>Value 37.</p><img src="'+image_path+'">'],'expected_page_count':1}},
                    {'type':'tool_use','id':'csv','name':'file_write','input':{'path':str(csv),'content':'Value\n37\n'}},*([{'type':'tool_use','id':'memo','name':'pdf_create','input':{'output_path':str(memo),'text_pages':['Separate plain memo. Value 37.']}}] if plain_sibling else [])]}

            if self.phase==4:
                return tool('file_read',{'path':'/outside/h839f.'+unknown_reference if unknown_reference else str(pdf)})
            if unknown_reference and self.phase==5:
                packets=[json.JSONDecoder().raw_decode(b['text'])[0] for m in kw['messages'] for block in m.get('content',[]) if isinstance(block,dict) and block.get('type')=='tool_result'
                    for b in block.get('content',[]) if b.get('type')=='text']
                recovery=next(p['output']['recovery'] for p in packets if isinstance(p.get('output'),dict)
                    and p['output'].get('reason')=='artifact_read_reference_selection_required')
                target=str(pdf) if unknown_reference=='pdf' else str(csv) if unknown_reference=='csv' else collected[0].output['image_paths'][0]
                assert target in recovery['candidate_paths'] and recovery['approval_bypass'] is False
                return tool('file_read',{'path':target})
            if unknown_reference in {'png','csv'} and self.phase==6:return tool('file_read',{'path':str(pdf)})
            assert self.phase==(5 if unknown_reference is None else 6 if unknown_reference=='pdf' else 7),'Reading a finished document triggered regeneration'
            return text('The prepared final PDF is ready for selection.')
    if plain_sibling: prompt += {'Create':' Also deliver a separate plain memo PDF.','Crea':' Entrega también un PDF de memorando separado.','値':' 別のメモPDFも添付してください。'}.get(prompt[:4], ' Also deliver a separate plain memo PDF.')
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        r=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:read-after-write'});assert r.status_code==200,r.text;data=r.json()
        record_property('runtime_evidence',json.dumps(runtime.store.list_conversation_events('web:read-after-write'),default=str))
        assert not model.errors,model.errors
        assert all(r.status=='completed' for r in writes),writes
        assert {row['path'] for row in data['artifacts']}=={str(pdf),str(csv),*([str(memo)] if plain_sibling else [])},data
        downloaded=client.get(next(row['url'] for row in data['artifacts'] if row['path']==str(pdf))).content
        assert client.get(next(row['url'] for row in data['artifacts'] if row['path']==str(csv))).content==b'Value\n37\n'
        document=PdfReader(io.BytesIO(downloaded))
        assert len(document.pages)==1 and len(document.pages[0].images)>0
        assert '37' in document.pages[0].extract_text()
        assert len(collected)==1 and len(reads)==(2 if unknown_reference in {'png','csv'} else 1) and len(writes)==(2 if plain_sibling else 1) and model.reviews==1 and all(x.status=='completed' for x in (*writes,*reads)), json.dumps(runtime.store.list_conversation_events('web:read-after-write'),default=str)
        assert not runtime.store.list_approval_requests(), 'A guessed path must not produce a spurious access request'

    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    reviews=[row for row in restored.store.list_conversation_events('web:read-after-write') if row.get('event_type')=='conversation.completion_review']
    assert reviews and reviews[-1]['disposition']=='complete' and reviews[-1].get('review_error') is None,reviews
    turns=restored.store.list_recent_conversation_events('web:read-after-write',event_type='conversation.chat_turn')
    assert turns[-1]['response_fulfilled'] is True, turns[-1]

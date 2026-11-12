"""Real Web/UI/Telegram/SQLite boundaries; only provider/source data are faked."""
import asyncio
from datetime import UTC,datetime
import json
import os

import pytest
from tests.e2e.test_universal_outcome_review_boundary import text

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')
NOTES=['The draft explanation is unsupported and needs to be omitted.',
       'La explicación del borrador debe eliminarse.',
       '下書きの未確認の説明を削除してください。']
FINDING='Selected hall: 20:30; 3 of 30 seats available.'


class Provider:
    def __init__(self,note,source,review_case="presentation_only",findings=None):
        self.findings=findings;self.note=note;self.source=source;self.review_case=review_case;self.reviews=0;self.calls=0;self.clock_packets=[]
    def create(self,**kw):
        system=str(kw.get('system') or '')
        names={t['name'] for t in kw.get('tools',[])}
        if 'material-requirements planner' in system:
            packet=json.loads(kw['messages'][0]['content'][0]['text']);self.clock_packets.append(packet)
            requirements=[{'requirement':'Report the selected paired measurements and any unconfirmed values' if self.findings is not None else 'Report the selected availability','requirement_kind':'fact',
                'evidence_subject_id':'selected','eligible_evidence_tool_names':['file_read']}]
            if self.findings is not None:
                requirements.append({'requirement':'Resolve all requested paired measurements','requirement_kind':'constraint',
                    'evidence_subject_id':'coverage','eligible_evidence_tool_names':['file_read']})
            if self.review_case=='invalid_matrix':
                requirements.append({**requirements[0],'requirement':'Also verify a different hall','evidence_subject_id':'different-hall'})
            return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                'material_requirements':requirements})
        if 'semantic completion reviewer' in system:
            self.reviews+=1
            packet=json.loads(kw['messages'][0]['content'][0]['text']);self.clock_packets.append(packet)
            ref=next(r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']=='file_read' and r['status']=='completed')
            return text({'disposition':'retry' if self.review_case=='presentation_only' else 'complete','all_material_requirements_enumerated':True,
                'unresolved_requirements':[self.note] if self.review_case=='presentation_only' else [],'user_facing_failure':self.note,'retry_tool_names':[],
                'source_presentation_valid':False,
                'supported_findings':[{**row,'evidence_invocation_ids':[ref],
                    'requirement_ids':[r['requirement_id'] for r in packet['material_requirement_contract']
                        if r['requirement_kind']=='constraint'] if row.get('kind')=='limitation' else []}
                    for row in self.findings] if self.findings is not None else [{'text':FINDING,'evidence_invocation_ids':[ref]}],
                'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':r['requirement_kind']!='constraint',
                    'draft_covered':self.review_case!='presentation_only','evidence_invocation_ids':[ref]} for r in packet['material_requirement_contract']]})
        if 'select_attachment_format' in names:
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':None,'confidence':1}}]}
        if not names:return text({})
        self.calls+=1
        def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use',
            'id':'call-'+str(self.calls),'name':name,'input':args}]}
        if 'file_read' not in names and self.calls<3:
            return call('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_read','file_search'] if self.review_case=='missing_required_tool' else ['file_read'],
                'required_tool_names':['file_read','file_search'] if self.review_case=='missing_required_tool' else ['file_read']})
        if 'file_read' in names and self.calls<=2:
            return call('file_read',{'path':str(self.source)})
        return text('Rejected draft: unsupported inventory explanation.')


@pytest.mark.parametrize('surface',['web_api','web_ui','telegram'])
@pytest.mark.parametrize('note',NOTES)
def test_review_notes_never_reach_ui_delivery_or_saved_history(tmp_path,monkeypatch,surface,note,structured=False):
    from fastapi.testclient import TestClient
    from nullion import chat_store,web_app,preferences,chat_operator
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings,TelegramSettings
    from nullion.messaging_turn_graph import PlatformChatRequest,run_platform_chat_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    for k in ['NULLION_MEMORY_ENABLED','NULLION_DOCTOR_ENABLED','NULLION_TASK_DECOMPOSITION_ENABLED','NULLION_MULTI_AGENT_ENABLED']:
        monkeypatch.setenv(k,'0')
    monkeypatch.setattr(preferences,'load_preferences',lambda:preferences.Preferences(timezone='Pacific/Honolulu'))
    chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');reads=[]
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    layout=[{'text':'Captured measurements for two pairs.','presentation':'paragraph'},
        {'text':'**Aoi + Ren**\nAoi: 173 cm; Ren: 183 cm. [Source](https://source.example/pair-a)',
            'presentation':'item','section':'Recorded pairs'},
        {'text':'**Luz + Sol**\nLuz: 164 cm; Sol: unverified. [Source](https://source.example/pair-b)',
            'presentation':'item','section':'Recorded pairs'},
        {'text':'Sol’s height was not established.','presentation':'paragraph','section':'Unconfirmed','kind':'limitation'}]
    expected=('Captured measurements for two pairs.\n\n**Recorded pairs**\n\n'
        '- **Aoi + Ren**\n  Aoi: 173 cm; Ren: 183 cm. [Source](https://source.example/pair-a)\n'
        '- **Luz + Sol**\n  Luz: 164 cm; Sol: unverified. [Source](https://source.example/pair-b)\n\n'
        '**Unconfirmed**\n\nSol’s height was not established.') if structured else FINDING
    needle='Aoi + Ren' if structured else FINDING
    source=tmp_path/'catalog.txt';source.write_text('\n'.join(row['text'] for row in layout) if structured else FINDING)
    from nullion.tools import _build_file_read_handler
    reader=_build_file_read_handler(tmp_path,allowed_roots=[tmp_path])
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,OPERATOR_PERMISSION_PRINCIPAL
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',OPERATOR_PERMISSION_PRINCIPAL,BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    def read(inv):
        reads.append(inv.invocation_id)
        return reader(inv)
    registry.register(ToolSpec('file_read','Read selected catalog data',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,3,capability_tags=('local_files',)),read)
    runtime.active_tool_registry=registry
    provider=Provider(note,source,findings=layout if structured else None);runner=AgentOrchestrator(model_client=provider)
    prompt=('各ペアの身長を比較し、未確認の値を明記してください: ' if structured else '選択したカタログを読み、今夜の空席を報告してください: ')+str(source)
    cid='telegram:42' if surface=='telegram' else 'web:publication'
    if surface=='telegram':
        settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='42',chat_enabled=True))
        result=run_platform_chat_request(runtime,PlatformChatRequest(platform='telegram',text=prompt,conversation_id=cid,
            chat_id='42',settings=settings,model_client=provider,agent_orchestrator=runner,append_activity_trace=False))
        answer=result.text
        from telegram import Bot,Message
        from nullion.telegram_app import _deliver_reply
        from tests.e2e.test_health_action_acknowledgements import telegram_boundary
        with telegram_boundary() as (url,calls):
            async def deliver():
                async with Bot('123456:synthetic',base_url=url) as bot:
                    message=Message.de_json({'message_id':101,'date':1,'chat':{'id':42,'type':'private'},'text':prompt},bot)
                    await _deliver_reply(message,answer)
            asyncio.run(deliver())
        wire=[payload['text'] for method,payload in calls if method=='sendMessage']
        assert len(wire)==1 and needle in wire[0] and note not in wire[0],wire
        if structured:
            assert '<b>Recorded pairs</b>' in wire[0] and '**' not in wire[0]
            assert wire[0].count('• ')==2 and 'https://source.example/pair-a' in wire[0]
    elif surface=='web_api':
        with TestClient(web_app.create_app(runtime,runner,registry)) as client:
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            assert result.status_code==200,result.text
            answer=result.json()['text']
    else:
        from playwright.sync_api import sync_playwright,expect
        from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port
        with _UvicornThread(web_app.create_app(runtime,runner,registry),_free_port()) as server:
            with sync_playwright() as p:
                browser=p.chromium.launch(headless=True);page=browser.new_page()
                page.goto(server.base_url,wait_until='domcontentloaded')
                page.locator('#user-input').fill(prompt)
                # Wait for the terminal bot-history write, not the first streamed
                # delta: reloading during a stream intentionally interrupts it.
                with page.expect_response(lambda response: '/api/chat/history/' in response.url
                        and response.url.endswith('/message')
                        and response.request.post_data_json.get('role') == 'bot', timeout=20000) as saved:
                    page.locator('#user-input').press('Enter')
                assert saved.value.ok
                expect(page.locator('#messages')).to_contain_text(needle,timeout=20000)
                if structured:
                    expect(page.locator('#messages li')).to_have_count(2)
                    expect(page.locator('#messages li').nth(0)).to_contain_text('173 cm')
                    expect(page.locator('#messages li').nth(1)).to_contain_text('unverified')
                answer=page.locator('#messages').inner_text()
                assert note not in answer and 'Rejected draft' not in answer
                page.reload(wait_until='domcontentloaded')
                expect(page.locator('#messages')).to_contain_text(needle,timeout=10000)
                assert note not in page.locator('#messages').inner_text()
                browser.close()
    assert needle in answer and note not in answer and 'Rejected draft' not in answer
    assert len(reads)==1 and 1<=provider.reviews<=3 and provider.calls>=4
    rows=[e for e in runtime.store.list_conversation_events() if e.get('event_type')=='conversation.chat_turn' and e.get('user_message')==prompt]
    assert rows and rows[-1]['assistant_reply']==expected
    for packet in provider.clock_packets:
        clock=packet.get('request_time_context')
        assert clock and clock['timezone']=='Pacific/Honolulu'
        from zoneinfo import ZoneInfo
        accepted=datetime.fromisoformat(clock['accepted_at_utc'])
        assert clock['accepted_local_date']==accepted.astimezone(ZoneInfo(clock['timezone'])).date().isoformat()
        assert clock==provider.clock_packets[0]['request_time_context']
    runtime.checkpoint(force=True)
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert any(e.get('assistant_reply')==expected for e in restored.store.list_conversation_events())


@pytest.mark.parametrize('review_case',['invalid_matrix','missing_required_tool'])
def test_runtime_rejections_preserve_independently_certified_findings(tmp_path,monkeypatch,review_case):
    from fastapi.testclient import TestClient
    from nullion import chat_store,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,
        _build_file_read_handler,_build_file_search_handler)
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,OPERATOR_PERMISSION_PRINCIPAL
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED','0');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    source=tmp_path/'catalog.txt';source.write_text(FINDING)
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);reads=[]
    reader=_build_file_read_handler(tmp_path,allowed_roots=[tmp_path])
    def read(inv):reads.append(inv.invocation_id);return reader(inv)
    registry.register(ToolSpec('file_read','Read selected file',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3),read)
    registry.register(ToolSpec('file_search','Check reference file',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3),
        _build_file_search_handler(tmp_path,allowed_roots=[tmp_path]))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    provider=Provider(NOTES[0],source,review_case)
    with TestClient(web_app.create_app(runtime,AgentOrchestrator(model_client=provider),registry)) as client:
        response=client.post('/api/chat',json={'text':'Read the catalog and verify the independent reference: '+str(source),
            'conversation_id':'web:runtime-rejection'})
        assert response.status_code==200,response.text
        payload=response.json()
        assert FINDING in payload['text'] and 'Rejected draft' not in payload['text'],payload
        assert NOTES[0] not in payload['text'] and not payload['artifacts']
    assert len(reads)==1
    rows=[e for e in runtime.store.list_conversation_events('web:runtime-rejection') if e.get('event_type')=='conversation.chat_turn']
    assert FINDING in rows[-1]['assistant_reply']
    assert any(e.get('event_type')=='conversation.completion_review' and e.get('disposition')=='retry'
        for e in runtime.store.list_conversation_events('web:runtime-rejection'))


@pytest.mark.parametrize('complete',[False,True])
def test_reviewed_file_caption_keeps_report_bytes_and_excludes_editing_notes(tmp_path,monkeypatch,complete):
    from nullion import chat_store
    from nullion.agent_orchestrator import AgentOrchestrator,TurnResult
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolInvocation,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    cid='telegram:42';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    path=root/'selected-report.html'
    content='<!doctype html><html><body><h1>Selected report</h1><p>'+FINDING+'</p></body></html>'
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path,root])
    registry.register(ToolSpec('file_write','Create selected report',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,3),
        _build_file_write_handler(tmp_path,allowed_roots=[tmp_path,root],include_principal_workspace=False))
    creation=registry.invoke(ToolInvocation(invocation_id='created',tool_name='file_write',principal_id=cid,arguments={'path':str(path),'content':content},capsule_id='fixture'))
    assert creation.status=='completed'
    class FileReview:
        def create(self,**kw):
            system=str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.html'],
                    'material_requirements':[{'requirement':'Deliver the selected report','requirement_kind':'deliverable',
                        'evidence_subject_id':'report','eligible_evidence_tool_names':['file_write']}]})
            if 'semantic completion reviewer' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                ref=next(r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']=='file_write')
                return text({'disposition':'complete' if complete else 'blocked','unresolved_requirements':[] if complete else [NOTES[1]],'retry_tool_names':[],
                    'user_facing_failure':None if complete else NOTES[1],'all_material_requirements_enumerated':True,
                    'supported_findings':[] if complete else [{'text':'Selected report created.','evidence_invocation_ids':[ref]}],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':complete,
                        'draft_covered':complete,'evidence_invocation_ids':[ref]} for r in packet['material_requirement_contract']]})
            return text({})
    reviewed=AgentOrchestrator(model_client=FileReview()).review_turn_result(
        TurnResult('created-turn','Selected report created.',tool_results=[creation],artifacts=[str(path)]),
        conversation_id=cid,principal_id=cid,user_message='Create the selected report as .html.',conversation_history=[],
        tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert reviewed.final_text.startswith('Selected report created.') and NOTES[1] not in reviewed.final_text
    assert path.read_bytes()==content.encode()
    if not complete:
        assert reviewed.artifacts == [str(path)] and reviewed.response_fulfilled is not True
        assert 'The requested output is not yet complete.' not in reviewed.final_text
        return
    assert reviewed.artifacts==[str(path)] and reviewed.response_fulfilled is True,reviewed
    from telegram import Bot,Message
    from nullion.telegram_app import _deliver_reply
    from nullion.messaging_adapters import delivery_contract_for_turn
    from tests.e2e.test_generated_artifact_protocol_recovery import upload_boundary
    contract=delivery_contract_for_turn(None,reply=reviewed.final_text,artifact_paths=reviewed.artifacts,
        requires_attachment_delivery=True)
    captions=[]
    with upload_boundary(messages=captions) as (url,uploads):
        async def deliver():
            async with Bot('123456:synthetic',base_url=url) as bot:
                message=Message.de_json({'message_id':101,'date':1,'chat':{'id':42,'type':'private'},'text':'Report'},bot)
                await _deliver_reply(message,reviewed.final_text,principal_id=cid,delivery_contract=contract,
                    artifact_paths=reviewed.artifacts,tool_results=reviewed.tool_results)
        asyncio.run(deliver())
    assert uploads==[content.encode()] and path.read_bytes()==content.encode()
    public=[payload.get('caption') or payload.get('text') or '' for method,payload in captions]
    assert any('Selected report created.' in value for value in public),public
    assert all(NOTES[1] not in value for value in public)

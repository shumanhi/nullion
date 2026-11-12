"""Real runner, persistence, Web UI and scheduled delivery; external sources/models scripted."""
import json
import os

import pytest

from tests.e2e.test_reviewed_failure_boundaries import isolated
from tests.e2e.test_universal_outcome_review_boundary import text
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


class Provider:
    def __init__(self, *, complete=True, bound=False):
        self.calls=0;self.complete=complete;self.bound=bound
    def create(self,**kw):
        auxiliary=workflow_provider_reply(kw,completion=False)
        if auxiliary is not None:return auxiliary
        system=kw.get('system','');names={t['name'] for t in kw.get('tools',[])}
        if 'material-requirements planner' in system:
            return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                'material_requirements':[{'requirement':'Report the selected current observations',
                    'requirement_kind':'fact','evidence_subject_id':'selected',
                    'eligible_evidence_tool_names':['observation_read']}]})
        if 'semantic completion reviewer' in system:
            packet=json.loads(kw['messages'][0]['content'][0]['text'])
            good=[r for r in packet['tool_evidence'] if r['tool_name']=='observation_read' and r['status']=='completed'
                and not r['output'].get('terminal_user_action_required')]
            bad=[r for r in packet['tool_evidence'] if r['tool_name']=='observation_read'
                and (r['status']=='failed' or r['output'].get('terminal_user_action_required'))]
            assert good and bad, [(r['tool_name'],r['status'],r['output']) for r in packet['tool_evidence']]
            return text({'disposition':'complete' if self.complete else 'blocked',
                'all_material_requirements_enumerated':True,'retry_tool_names':[],
                'unresolved_requirements':[] if self.complete else ['Private editing instruction'],
                'user_facing_failure':None if self.complete else 'Private editing instruction',
                'supported_findings':[] if self.complete else [{'text':'確認済みの値: 7。',
                    'evidence_invocation_ids':[good[-1]['invocation_id']]}],
                'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':self.complete,
                    'draft_covered':self.complete,'evidence_invocation_ids':[good[-1]['invocation_id']] if self.complete else [],
                    'blocking_failure_invocation_ids':[bad[-1]['invocation_id']] if self.bound else []}
                    for r in packet['material_requirement_contract']]})
        if 'select_attachment_format' in names:
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format',
                'input':{'extension':None,'confidence':1}}]}
        if not names:return text({})
        if 'observation_read' not in names and 'request_tool_scope' in names:
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                'input':{'capabilities':['web'],'tool_names':['observation_read']}}]}
        self.calls+=1
        if self.calls<=2:
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':f'probe-{self.calls}',
                'name':'observation_read','input':{'target':str(self.calls)}}]}
        return text('確認済みの値: 7。')


def setup(tmp_path,monkeypatch,*,pending=False,**provider_kwargs):
    from nullion.tools import ToolRegistry,ToolSpec,ToolSideEffectClass,ToolRiskLevel,ToolResult
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime,home=isolated(tmp_path,monkeypatch);registry=ToolRegistry();attempts=[]
    def probe(inv):
        attempts.append(inv.arguments)
        if inv.arguments['target']=='1':
            return ToolResult(inv.invocation_id,inv.tool_name,'completed' if pending else 'failed',{'connection_state':'pending_or_failed',
                'connector_app_id':'selected-source','reason':'source_connection_required','retryable':False,
                **({'terminal_user_action_required':True} if pending else {})},None if pending else 'PRIVATE')
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':7,'text':'確認済みの値: 7。'})
    registry.register(ToolSpec('observation_read','Read the selected observation source',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2, capability_tags=('web',), input_schema={'type':'object', 'properties':{'target':{'type':'string'}},'required':['target'],'additionalProperties':False}),probe)
    runtime.active_tool_registry=registry;provider=Provider(**provider_kwargs)
    return runtime,registry,provider,AgentOrchestrator(model_client=provider),attempts


@pytest.mark.parametrize('prompt',['選択した情報を確認して。','Consulta los datos seleccionados.'])
@pytest.mark.parametrize('complete,bound',[(True,False),(False,False),(False,True)])
def test_web_reply_keeps_findings_and_distinguishes_current_connector_blockers(tmp_path,monkeypatch,prompt,complete,bound):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    runtime,registry,provider,runner,attempts=setup(tmp_path,monkeypatch,complete=complete,bound=bound)
    with TestClient(create_app(runtime,runner,registry)) as client:
        r=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:outcomes'})
        assert r.status_code==200,r.text
        answer=r.json()['text'];assert '確認済みの値: 7。' in answer, json.dumps([
            {k:e.get(k) for k in ('event_type','disposition','review_error','response_diagnostics','supported_finding_count','runtime_failure_code','assistant_reply','tool_results')}
            for e in runtime.store.list_conversation_events('web:outcomes')
            if e.get('event_type') in {'conversation.completion_review','conversation.chat_turn'}])
        assert 'Private editing instruction' not in answer and 'PRIVATE' not in answer
        assert ('Reconnect' in answer) is bound
    turns=[e for e in runtime.store.list_conversation_events('web:outcomes') if e.get('event_type')=='conversation.chat_turn']
    assert turns and answer==turns[-1]['assistant_reply']
    results=turns[-1]['tool_results'];assert len([r for r in results if r['tool_name']=='observation_read'])==2
    assert len(attempts)==2
    assert any(r.get('response_requirements',{}).get('execution_review') for r in results)


def test_web_ui_reload_keeps_successful_reply_after_old_connector_failure(tmp_path,monkeypatch):
    from playwright.sync_api import sync_playwright,expect
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port
    runtime,registry,provider,runner,attempts=setup(tmp_path,monkeypatch)
    with _UvicornThread(create_app(runtime,runner,registry),_free_port()) as server, sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        page=browser.new_page();page.goto(server.base_url)
        page.locator('#user-input').fill('選択した情報を確認して。')
        page.locator('#user-input').press('Enter')
        expect(page.locator('#messages')).to_contain_text('確認済みの値: 7。',timeout=20000)
        page.reload();expect(page.locator('#messages')).to_contain_text('確認済みの値: 7。',timeout=20000)
        assert 'Reconnect' not in page.locator('#messages').inner_text()
        browser.close()
    assert len(attempts)==2


def test_completed_adapter_reconnect_flag_remains_a_current_blocker(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    runtime,registry,provider,runner,attempts=setup(tmp_path,monkeypatch,complete=False,bound=True,pending=True)
    with TestClient(create_app(runtime,runner,registry)) as client:
        result=client.post('/api/chat',json={'text':'選択した情報を確認して。','conversation_id':'web:pending-adapter'})
        assert result.status_code==200
        answer=result.json()['text']
        assert '確認済みの値: 7。' in answer and 'Reconnect' in answer, [
            {k:e.get(k) for k in ('event_type','disposition','review_error','supported_finding_count','assistant_reply','tool_results')}
            for e in runtime.store.list_conversation_events('web:pending-adapter')
            if e.get('event_type') in {'conversation.completion_review','conversation.chat_turn'}]
    assert len(attempts)==2


def test_scheduled_report_delivers_success_and_records_success_despite_historical_failure(tmp_path,monkeypatch):
    from nullion.crons import CronJob
    from tests.e2e.test_scheduled_report_completion import run_scheduled,telegram_boundary
    runtime,registry,provider,runner,attempts=setup(tmp_path,monkeypatch)
    deliveries,send=telegram_boundary(monkeypatch)
    job=CronJob(id='selected-observation',name='定期確認',task='選択した情報を確認して。',schedule='0 * * * *',
        delivery_channel='telegram',delivery_target='123')
    result=run_scheduled(job,provider,runtime,registry,send)
    assert len(deliveries)==1 and '確認済みの値: 7。' in deliveries[0]['text']
    assert 'Reconnect' not in deliveries[0]['text']
    assert result['execution_outcome']['status']=='succeeded',result['execution_outcome']
    assert len(attempts)==2


def test_native_image_provider_timeout_reaches_safe_user_notice(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from datetime import UTC,datetime
    from nullion.tools import ToolRegistry,register_media_plugin
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL,BoundaryPolicyRule,BoundaryKind,PolicyMode
    runtime,home=isolated(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED','1')
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));calls=[]
    def generate(*args):calls.append(args);raise TimeoutError('PRIVATE upstream token=xyz')
    register_media_plugin(registry,image_generator=generate)
    runtime.active_tool_registry=registry
    runtime.store.add_permission_grant(create_permission_grant(approval_id='image-permission',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL,permission='tool:image_generate',
        granted_by='fixture',granted_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('image-files',OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    class ImageProvider:
        calls=0
        def create(self,**kw):
            auxiliary=workflow_provider_reply(kw,completion=False)
            if auxiliary is not None:return auxiliary
            system=kw.get('system','');names={t['name'] for t in kw.get('tools',[])}
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.png'],
                    'material_requirements':[{'requirement':'Deliver the requested generated image',
                        'requirement_kind':'deliverable','evidence_subject_id':'.png',
                        'eligible_evidence_tool_names':['image_generate']}]})
            if 'semantic completion reviewer' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                bad=next(r for r in packet['tool_evidence'] if r['tool_name']=='image_generate' and r['status']=='failed')
                assert bad['output']['reason']=='handler_timeout'
                return text({'disposition':'blocked','all_material_requirements_enumerated':True,
                    'unresolved_requirements':['Image unavailable'],'user_facing_failure':'Private repair instructions',
                    'retry_tool_names':[],'supported_findings':[{'kind':'limitation',
                        'text':'The file-generation step timed out. The image was not created.',
                        'evidence_invocation_ids':[bad['invocation_id']]}],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':False,
                        'draft_covered':False,'evidence_invocation_ids':[],
                        'blocking_failure_invocation_ids':[bad['invocation_id']]}
                        for r in packet['material_requirement_contract']]})
            if 'select_attachment_format' in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format',
                    'name':'select_attachment_format','input':{'extension':'.png','confidence':1}}]}
            if not names:return text({})
            if 'image_generate' not in names and 'request_tool_scope' in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope',
                    'name':'request_tool_scope','input':{'capabilities':['image_generation'],
                        'tool_names':['image_generate'],'artifact_extensions':['.png']}}]}
            self.calls+=1
            if self.calls==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'image',
                    'name':'image_generate','input':{'prompt':'抽象的な風景',
                        'output_path':str(tmp_path/'output.png')}}]}
            return text('The image could not be created.')
    provider=ImageProvider()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=provider),registry)) as client:
        result=client.post('/api/chat',json={'conversation_id':'web:image-timeout','text':'抽象的な風景を画像にしてください。'})
        assert result.status_code==200,result.text
        answer=result.json()['text']
        assert 'timed out' in answer and 'PRIVATE' not in answer and 'API key' not in answer
    assert len(calls)==1 and not (tmp_path/'output.png').exists()
    turns=[e for e in runtime.store.list_conversation_events('web:image-timeout') if e.get('event_type')=='conversation.chat_turn']
    assert turns[-1]['assistant_reply']==answer
    assert any(r['tool_name']=='image_generate' and r['status']=='failed'
        and r['output']['reason']=='handler_timeout' for r in turns[-1]['tool_results'])

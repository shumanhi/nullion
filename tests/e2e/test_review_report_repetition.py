"""A partial helper report cannot duplicate the producer at real delivery."""
import asyncio
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, telegram_boundary, run_scheduled, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker workflow boundary')


@pytest.mark.parametrize('surface', ['interactive', 'scheduled'])
@pytest.mark.parametrize('primary,alternate,limitation', [
    ('要点\nA: 7\nB: 8', '再構成\nAは7です\nBは8です', '予定表に重複が残っています。'),
    ('Resumen\nA: 7\nB: 8', 'Otra versión\nA tiene 7\nB tiene 8', 'Queda un recordatorio duplicado.'),
])
def test_partial_review_keeps_one_report_and_one_narrow_limitation(tmp_path, monkeypatch, surface, primary, alternate, limitation):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_text import REPORT_MESSAGE_FORMAT
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime=setup_runtime(tmp_path,monkeypatch);registry=ToolRegistry();reads=[]
    def read(call):
        reads.append(call)
        return ToolResult(call.invocation_id,call.tool_name,'completed',{
            'value':7,'other_value':8,'duplicate_exists':True})
    registry.register(ToolSpec('record_read','Read the selected records',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2),read)
    class Provider:
        rounds=0
        def create(self, **kw):
            system=kw.get('system') or ''
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated':True,
                    'requested_artifact_extensions':[], 'material_requirements':[
                        {'requirement':'Report selected values','requirement_kind':'fact','evidence_subject_id':'values',
                         'eligible_evidence_tool_names':['record_read']},
                        {'requirement':'Retain a single matching reminder','requirement_kind':'constraint',
                         'delivery_required':False,'evidence_subject_id':'reminder',
                         'eligible_evidence_tool_names':['record_read']}]}))
            if 'semantic completion reviewer' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                ref=next(row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name']=='record_read')
                rows=packet['material_requirement_contract']
                report={'summary':alternate,'detail_level':'compact','sections':[],'sources':[],'shared_details':[]}
                return text_response(json.dumps({'disposition':'blocked','retry_tool_names':[],
                    'all_material_requirements_enumerated':True,'unresolved_requirements':['Duplicate remains'],
                    'material_requirements':[{'requirement_id':row['requirement_id'],'satisfied':i==0,
                        'draft_covered':i==0,'evidence_invocation_ids':[ref]} for i,row in enumerate(rows)],
                    'supported_findings':[
                        {'text':{'format':REPORT_MESSAGE_FORMAT,'message':report},'presentation':'paragraph',
                         'evidence_invocation_ids':[ref]},
                        {'text':limitation,'kind':'limitation','presentation':'paragraph',
                         'requirement_ids':[rows[1]['requirement_id']],'evidence_invocation_ids':[ref]}]}))
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            self.rounds+=1
            return tool_response('record_read',{}) if self.rounds==1 else text_response(primary)
    provider=Provider();deliveries,send=telegram_boundary(monkeypatch)
    if surface=='scheduled':
        result=run_scheduled(CronJob(id='repeated-report',name='資料',task='選択した記録と既存通知を確認。',
            schedule='0 * * * *',delivery_channel='telegram',delivery_target='123'),provider,runtime,registry,send)
        assert result['cron_delivery_status']=='sent'
    else:
        result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='telegram:123',principal_id='telegram:123',
            user_message='Comprueba los registros y conserva un solo aviso.',conversation_history=[],tool_registry=registry,
            policy_store=runtime.store,approval_store=runtime.store,max_iterations=5)
        from nullion.telegram_app import _send_operator_telegram_message
        assert asyncio.run(_send_operator_telegram_message('fixture','123',result.final_text,principal_id='telegram:123'))
    visible='\n'.join(d.get('text') or d.get('caption') or '' for d in deliveries)
    assert len(reads)==1 and len(deliveries)==1
    assert visible.count(primary)==1 and alternate not in visible
    reviews=runtime.store.list_recent_conversation_events('telegram:123',event_type='conversation.completion_review')
    assert visible.count(limitation)==1, json.dumps(reviews,ensure_ascii=False)

"""Native record reports use their applicable review and one delivery title."""
import json
import os

import pytest

from tests.report_presentation_fixture import report_fixture
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import setup_runtime,telegram_boundary,run_scheduled,text_response,tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container product boundary')


@pytest.mark.parametrize('task',['記録と出典、期限、金額、予定表の結果を報告。','Resume avisos, fuentes, fechas, importes y calendario.'])
@pytest.mark.parametrize('detail',['compact','detailed'])
def test_native_review_correction_reaches_scheduled_delivery_without_inapplicable_source_gate(tmp_path,monkeypatch,task,detail,triage=False):
    from nullion import preferences
    from nullion.crons import CronJob
    from nullion.chat_text import REPORT_MESSAGE_FORMAT,render_report_message
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    runtime=setup_runtime(tmp_path,monkeypatch)
    preferences.save_preferences(preferences.Preferences(response_length='detailed' if detail=='detailed' else 'balanced',
        emoji_level='expressive'))
    if triage:
        from tests.triage_compact_fixture import triage_fixture
        message,verbose,source=triage_fixture()
    else:message,verbose,source=report_fixture()
    message['detail_level']=detail;message['title']='記録の要点'
    expected=render_report_message(message,include_title=False)
    deliveries,send=telegram_boundary(monkeypatch);registry=ToolRegistry();reads=[]
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id,call.tool_name,'completed',{'value':7,**source})
    registry.register(ToolSpec('record_read','Read native retained obligation records',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2),read)
    class Provider:
        reviews=0
        def create(self,**kwargs):
            system=str(kwargs.get('system') or '')
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:
                if 'semantic completion reviewer' in system:
                    self.reviews+=1;packet=json.loads(kwargs['messages'][0]['content'][0]['text'])
                    assert packet['source_answer_selection'] is None
                    assert packet['report_presentation_contract']['include_title'] is False
                    reply=json.loads(protocol['content'][0]['text'])
                    ids=[r['requirement_id'] for r in reply['material_requirements']]
                    refs=list(dict.fromkeys(ref for r in reply['material_requirements'] for ref in r['evidence_invocation_ids']))
                    reply.update(reply_target='corrected_findings',source_presentation_valid=None,
                        supported_findings=[{'text':{'format':REPORT_MESSAGE_FORMAT,'message':message},
                            'requirement_ids':ids,'evidence_invocation_ids':refs,'presentation':'paragraph'}])
                    return text_response(json.dumps(reply))
                return protocol
            if not reads:return tool_response('record_read',{})
            return text_response(verbose)
    model=Provider();job=CronJob(id='native-report',name='記録の要点',task=task,schedule='0 * * * *',
        delivery_channel='telegram',delivery_target='123')
    result=run_scheduled(job,model,runtime,registry,send)
    assert result['response_fulfilled'] is True and len(reads)==model.reviews==1
    assert len(deliveries)==1 and deliveries[0]['text'].count(job.name)==1
    from nullion.telegram_formatting import telegram_html_to_plain
    body=telegram_html_to_plain(deliveries[0]['text'])
    for source_row in message['sources']:assert source_row['label'] in body
    if triage:
        assert all(value in body for value in ('$420', '$460', '$150', '$2,000', '$1,000', '11:00 PM', 'placeholder'))
        assert ('Full optional notification wording' in body)==(detail=='detailed')
        assert 'Search chains' not in body
    else:assert '$720' in body and '$760' in body and 'Final posting is unconfirmed' in body
    assert body.count(message['shared_details'][0]['text'])==1
    if not triage:assert ('Historical detail' in body)==(detail=='detailed')
    if detail=='compact':assert len(body)<len(verbose)*.65
    review=runtime.store.list_recent_conversation_events('telegram:123',event_type='conversation.completion_review')[-1]
    assert review['review_error'] is None and review['presentation_repaired'] and review['disposition']=='complete'


@pytest.mark.parametrize('detail',['compact','detailed'])
def test_native_obligation_report_groups_routine_outcomes_and_provenance(tmp_path,monkeypatch,detail):
    test_native_review_correction_reaches_scheduled_delivery_without_inapplicable_source_gate(
        tmp_path,monkeypatch,'報告の要点と必要な対応を示してください。',detail,triage=True)

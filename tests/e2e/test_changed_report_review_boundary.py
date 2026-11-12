"""Real scheduled producer, native file tools, SQLite and delivered file bytes."""
import json
import os
from pathlib import Path

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E')


@pytest.mark.parametrize('task',['確認結果を .html にまとめ、要約と一緒に送付してください。',
    'Entrega los registros en .html junto con un resumen.'])
@pytest.mark.parametrize('variant',['file_changed','evidence_only','outage','blocked'])
def test_recovered_report_requires_current_evidence_and_file_review(tmp_path,monkeypatch,task,variant):
    from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response, tool_response
    from nullion.artifacts import artifact_root_for_principal
    from nullion.crons import CronJob
    from nullion.tools import (ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,
        _build_file_read_handler,_build_file_write_handler)
    runtime=setup_runtime(tmp_path,monkeypatch)
    artifact=Path(artifact_root_for_principal('telegram:123'))/'selected.html'
    artifact.parent.mkdir(parents=True,exist_ok=True)
    source_a=tmp_path/'source-a.txt';source_a.write_text('A = 7')
    source_b=tmp_path/'source-b.txt'
    if variant!='blocked':source_b.write_text('B = 8')
    complete='<!doctype html><html><body><h1>Selected values</h1><p>A: 7; B: 8.</p></body></html>'
    partial=complete.replace('B: 8.','B: pending.')
    summary='Selected values: A = 7; B = 8. Report attached.'
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));reads=[];writes=[]
    native_read=_build_file_read_handler(None,allowed_roots=[tmp_path])
    native_write=_build_file_write_handler(None,allowed_roots=[tmp_path])
    def read(inv):reads.append(Path(inv.arguments['path']));return native_read(inv)
    def write(inv):writes.append(Path(inv.arguments['path']));return native_write(inv)
    registry.register(ToolSpec('file_read','Read selected local evidence',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2,input_schema={'type':'object','properties':{'path':{'type':'string'}},'required':['path']}),read)
    registry.register(ToolSpec('file_write','Write selected report',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,2,input_schema={'type':'object','properties':{'path':{'type':'string'},'content':{'type':'string'}},'required':['path','content']}),write)
    class Provider:
        reviews=0
        diagnostics=[]
        def create(self,**kw):
            system=kw.get('system') or ''
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.html'],
                    'material_requirements':[
                        {'requirement':'Verify both selected records','requirement_kind':'fact','evidence_subject_id':'records','eligible_evidence_tool_names':['file_read']},
                        {'requirement':'Deliver the selected HTML report','requirement_kind':'deliverable','evidence_subject_id':'.html','eligible_evidence_tool_names':['file_write']}]}))
            if 'semantic completion reviewer' in system:
                self.reviews+=1
                if source_b in reads and variant=='outage':raise TimeoutError('Synthetic independent reviewer outage')
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                self.diagnostics.append({'review':self.reviews,'protocol':packet.get('runtime_coverage_validation_errors'),'draft':packet.get('draft_final_response')})
                ready=source_b in reads and variant!='blocked' and artifact.read_text()==complete
                blocked=source_b in reads and variant=='blocked'
                correct=packet['draft_final_response']==summary
                finished=ready and correct
                rows=[]
                for r in packet['material_requirement_contract']:
                    eligible=[e for e in packet['tool_evidence'] if e['status']=='completed' and e['tool_name'] in r['eligible_evidence_tool_names']]
                    if r['requirement_kind']=='deliverable':
                        bound=[e for e in packet['tool_evidence'] if (e.get('output') or {}).get('artifact_record_binding')]
                        if bound:eligible=bound
                    rows.append({'requirement_id':r['requirement_id'],'satisfied':ready,'draft_covered':ready and correct,
                        'evidence_invocation_ids':[e['invocation_id'] for e in eligible[-1:]]})
                return text_response(json.dumps({'disposition':'complete' if finished else 'blocked' if blocked else 'retry',
                    'all_material_requirements_enumerated':True,'material_requirements':rows,
                    'unresolved_requirements':[] if finished else ['Present the verified records in the chat summary.' if ready else 'The second selected record is unavailable.' if blocked else 'Read the second selected record and update the report.'],
                    'retry_tool_names':[] if ready or blocked else ['file_read','file_write']}))
            if system.startswith('You repair scheduled-task final answers'):
                prompt=kw['messages'][0]['content'][0]['text']
                records=json.JSONDecoder().raw_decode(prompt.split('Verified compact tool evidence from this run:\n',1)[1])[0]
                return text_response(json.dumps({'requirements_satisfied':True,'source_state':'available',
                    'evidence_refs':[r['evidence_id'] for r in records if r['status']=='completed'], 'report':summary}))
            auxiliary=workflow_provider_reply(kw)
            if auxiliary is not None:return auxiliary
            if source_a not in reads:return tool_response('file_read',{'path':str(source_a)})
            if not artifact.exists():return tool_response('file_write',{'path':str(artifact),'content':complete if variant=='evidence_only' else partial})
            if self.reviews and source_b not in reads:return tool_response('file_read',{'path':str(source_b)})
            if source_b in reads and variant not in {'blocked','evidence_only'} and artifact.read_text()!=complete:
                return tool_response('file_write',{'path':str(artifact),'content':complete})
            return text_response(summary if source_b in reads and variant!='blocked'
                else 'A = 7; the second selected record remains unverified.')
    model=Provider();deliveries,send=telegram_boundary(monkeypatch)
    result=run_scheduled(CronJob(id='review-final-version',name='確認',task=task,schedule='* * * * *',
        delivery_channel='telegram',delivery_target='123',artifact_delivery_options={'requires_attachment_delivery':True,
            'required_artifact_extensions':['.html']}),model,runtime,registry,send)
    uploads=[d for d in deliveries if 'bytes' in d]
    if not uploads:
        Path('/outputs/cron-duplicate-delivery-20261103/review-debug.json').write_text(json.dumps({
            'result':result,'reads':list(map(str,reads)),'writes':list(map(str,writes)),
            'reviews':model.diagnostics,'deliveries':deliveries},default=str,indent=2))
    assert len(uploads)==1 and uploads[0]['bytes']==artifact.read_bytes(),(result,deliveries)
    assert reads==[source_a,source_b] and model.reviews>=2,(reads,model.reviews,result)
    success=variant in {'file_changed','evidence_only'}
    if variant == 'outage':
        assert result['response_fulfilled'] is not True, result
    else:
        assert result['response_fulfilled'] is success,result
    assert result['execution_outcome']['completion_verified'] is success,result
    assert len(writes)==(2 if variant in {'file_changed','outage'} else 1)
    reviews=runtime.store.list_recent_conversation_events('telegram:123',event_type='conversation.completion_review')
    if success:
        assert reviews[-1]['disposition']=='complete'
        assert 'A: 7; B: 8.' in uploads[0]['bytes'].decode()
        assert result['execution_outcome']['report_content_status']=='complete'
        assert not result.get('cron_report_partial')
        assert summary in uploads[0]['caption']
    else:
        assert 'A: 7;' in uploads[0]['bytes'].decode()
        assert result['execution_outcome']['report_content_status'] in {'partial','unknown'}
        if variant=='outage':
            assert result['execution_outcome']['review_status']=='unavailable'
            # Helper availability is internal, while actual unresolved work
            # keeps its existing partial/unknown state and usable attachment.
            assert 'final review' not in uploads[0]['caption']
            assert summary in uploads[0]['caption']
            assert 'Partial report attached.' not in uploads[0]['caption']
    visible=' '.join(str(d.get('caption') or d.get('text') or '') for d in deliveries)
    assert 'Read the second selected record and update' not in visible

"""Scheduled HTML review outage preserves delivery without grading unchanged bytes again."""
import json
import os
import time
from datetime import UTC, datetime

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, telegram_boundary, run_scheduled, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker workflow boundary')


@pytest.mark.parametrize('mode',['timeout','critique','inspection_timeout'])
@pytest.mark.parametrize('task',['観測を確認し、HTML資料を添付してください。','Comprueba los valores y adjunta un informe HTML.'])
def test_report_reviewer_outage_does_not_repeat_on_runtime_caption_normalization(tmp_path,monkeypatch,task,record_property,mode):
    from nullion.crons import CronJob
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,BoundaryPolicyRule,PolicyMode
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult,_build_file_write_handler
    from nullion.chat_text import REPORT_MESSAGE_FORMAT, report_presentation_instructions
    from nullion import agent_orchestrator
    spent = [0.0]
    class ReviewClock:
        def __getattr__(self, name): return getattr(time, name)
        def perf_counter(self): return time.perf_counter() + spent[0]
    if mode == 'inspection_timeout':
        monkeypatch.setattr(agent_orchestrator, 'time', ReviewClock())
        monkeypatch.setenv('NULLION_COMPLETION_REVIEW_TIMEOUT_SECONDS', '6')
    runtime=setup_runtime(tmp_path,monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fs',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.FILESYSTEM_ACCESS,mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));reads=[]
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id,call.tool_name,'completed',{'value':49,'unique_visitors':14})
    registry.register(ToolSpec('record_read','Read selected metrics',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    registry.register(ToolSpec('file_write','Write selected report',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),
        _build_file_write_handler(allowed_roots=(tmp_path,)))
    path=artifact_root_for_principal('telegram:2222')/'report.html'
    html='<!doctype html><html><body><h1>49 views</h1><p>14 unique visitors</p></body></html>'
    class Provider:
        producer=0;reviews=0
        review_timeouts=[]
        def create(self,**kw):
            system=kw.get('system') or ''
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1
                self.review_timeouts.append(kw['timeout'])
                if mode == 'inspection_timeout':
                    if self.reviews == 1:
                        packet=json.loads(kw['messages'][0]['content'][0]['text'])
                        ref=next(row['invocation_id'] for row in packet['tool_evidence']
                            if row['tool_name']=='record_read')
                        spent[0]+=1.0
                        return text_response(json.dumps({'disposition':'inspect_evidence',
                            'evidence_requests':[{'invocation_id':ref,'json_pointer':'','length':1000}]}))
                    spent[0]+=kw['timeout']
                    from nullion.model_clients import ModelClientTimeoutError
                    raise ModelClientTimeoutError('Synthetic elapsed review deadline')
                if mode=='critique':
                    return text_response(json.dumps({'disposition':'blocked','retry_tool_names':[],
                        'unresolved_requirements':['An editorial preference remains.']}))
                time.sleep(.02)
                from nullion.model_clients import ModelClientTimeoutError
                raise ModelClientTimeoutError('Synthetic unavailable review')
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text_response('{}')
            self.producer+=1
            visible='\n'.join(block.get('text','') for row in kw['messages']
                for block in row.get('content',[]) if isinstance(block,dict))
            assert report_presentation_instructions() in visible
            assert '"delivery_role": "artifact_summary"' in visible
            if self.producer==1:return tool_response('record_read',{})
            if self.producer==2:return tool_response('file_write',{'path':str(path),'content':html})
            assert self.producer==3,'Completed research was restarted after review outage'
            message={'summary':'49 views from 14 visitors; unchanged since the previous snapshot.',
                'detail_level':'compact','sections':[],'sources':[],'shared_details':[],
                'extra_details':['Authentication succeeded; charts contain numeric rows; full methods and sources are in the file.']}
            return text_response(json.dumps({'format':REPORT_MESSAGE_FORMAT,'message':message})+'\nMEDIA:'+str(path))
    provider=Provider();deliveries,send=telegram_boundary(monkeypatch)
    job=CronJob(id='report-review-outage',name='資料',task=task,schedule='0 * * * *',
        delivery_channel='telegram',delivery_target='2222',artifact_delivery_options={
            'requires_attachment_delivery':True,'required_artifact_extensions':['.html']})
    started=time.monotonic()
    result=run_scheduled(job,provider,runtime,registry,send)
    elapsed=time.monotonic()-started
    record_property('review_outage_workflow_seconds',elapsed)
    assert elapsed<3
    assert provider.reviews==(1 if mode=='critique' else 2) and provider.producer==3 and len(reads)==1
    if mode == 'inspection_timeout':
        assert provider.review_timeouts[1] < provider.review_timeouts[0] - .9
        assert spent[0] <= 6.01, 'Inspection and retries must share one total allowance'
    assert len(deliveries)==1 and b'49 views' in deliveries[0]['bytes']
    assert b'14 unique visitors' in deliveries[0]['bytes']
    assert result['response_presentation']['outcome_review_status']==('advice' if mode=='critique' else 'unavailable')
    assert '49 views from 14 visitors' in deliveries[0]['caption']
    assert 'Authentication succeeded' not in deliveries[0]['caption']
    assert len(deliveries[0]['caption'].split()) < 40
    assert result['response_fulfilled'] is None
    assert result['response_presentation']['outcome_review_role']=='helper'
    assert 'unconfirmed' not in deliveries[0]['caption'] and 'Review warning' not in deliveries[0]['caption']
    assert result['execution_outcome']['delivery_succeeded'] is True
    assert result['execution_outcome']['status']=='partially_succeeded'
    assert result['execution_outcome']['failure_reason'] is None
    assert not result.get('cron_report_partial')
    assert 'Partial report' not in deliveries[0]['caption']
    assert result['execution_outcome']['completion_verified'] is False
    assert not result.get('cron_completion_continuations')
    # The actual acknowledged report result must not turn an unavailable
    # helper into twelve failed source checks or a Doctor incident.
    from datetime import timedelta
    from nullion.tracker_status import start_check,finish_check,tracker_statuses,dispatch_tracker_updates
    now=datetime.now(UTC)
    for index in range(12):
        at=now+timedelta(seconds=index*2);identity='helper-'+str(index)
        start_check(runtime.checkpoint_path,job,identity,now=at)
        finish_check(runtime.checkpoint_path,job,identity,result,now=at+timedelta(seconds=1))
    facts=tracker_statuses(runtime.checkpoint_path,[job],now=now+timedelta(seconds=25))[job.id]
    assert facts['state']=='delivered' and facts['consecutive_failures']==0
    assert facts['last_success_at'] is None
    assert dispatch_tracker_updates(runtime.checkpoint_path,[job],now=now+timedelta(seconds=25),
        resolve_route=lambda _job:('telegram','2222'),
        send=lambda *args:pytest.fail('A helper outage must not generate a failed-task alert'))==0

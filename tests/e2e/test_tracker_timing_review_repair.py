"""Repair wrong tracker timing through the real graph, tools and SQLite."""
from datetime import datetime, timedelta
import json
import os

import pytest

from tests.test_live_tracking import fixture, matcher
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('prompt',['15分ごとに3時間監視してください。','Comprueba cada 15 minutos durante tres horas.'])
def test_primary_declares_full_duration_without_rebuilding_the_source(fixture,prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.turn_context_policy import TurnToolScopeDecision
    f=fixture;f.registry.live_tracker_store_path=f.store.path;register_live_tracker_tools(f.registry)
    # This graph receives the structured plan selected by its chat owner.
    f.registry.turn_tool_scope_decision=TurnToolScopeDecision(tracking_mode='live',valid=True,
        requested_tool_names=('sensor_read','create_live_tracker'),required_tool_names=('create_live_tracker',))
    def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
    def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name+str(model.steps),'name':name,'input':args}]}
    class Provider:
        steps=0
        reviews=0
        def create(self,**kw):
            system=str(kw.get('system',''))
            if 'material-requirements planner' in system:
                return text(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'Configure a 10800-second observation every 900 seconds',
                        'requirement_kind':'action','evidence_subject_id':'observation-setup',
                        'eligible_evidence_tool_names':['create_live_tracker','update_live_tracker']}]}))
            if 'semantic completion reviewer' in system:
                self.reviews+=1;p=json.loads(kw['messages'][0]['content'][0]['text'])
                receipts=p['accepted_background_execution'];assert receipts
                latest=receipts[-1];config=latest['configuration'];complete=config.get('duration_seconds')==10800
                if complete:assert datetime.fromisoformat(config['expires_at'])-datetime.fromisoformat(config['accepted_at'])==timedelta(hours=3)
                return text(json.dumps({'disposition':'complete' if complete else 'retry',
                    'all_material_requirements_enumerated':True,
                    'unresolved_requirements':[] if complete else ['Amend the saved tracker to a full 10800-second window'],
                    'retry_tool_names':[] if complete else ['update_live_tracker'],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':complete,'draft_covered':complete,
                        'evidence_invocation_ids':[latest['invocation_id']] if complete else []}
                        for r in p['material_requirement_contract']]}))
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            self.steps+=1
            if self.steps==1:return tool('sensor_read',{})
            if self.steps==2:return tool('create_live_tracker',{'name':'観測','duration_seconds':10800,'interval_seconds':900,
                'probe':[{'tool_name':'sensor_read','arguments':{}}],'matcher':matcher('greater_than',99)})
            if self.reviews and len(f.store.list('workspace_admin'))==1 and self.steps==4:
                ident=f.store.list('workspace_admin')[0]['id']
                assert ident in json.dumps(kw['messages']),'The real model context must contain its accepted tracker id'
                return tool('update_live_tracker',{'id':ident,'duration_seconds':10800})
            return text('Configured for three hours, checking every fifteen minutes.')
    model=Provider()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:alpha',principal_id='web:alpha',
        user_message=prompt,conversation_history=[],tool_registry=f.registry,policy_store=f.runtime.store,
        approval_store=f.runtime.store,max_iterations=8)
    assert result.response_fulfilled is True, json.dumps({'text':result.final_text,
        'failure':result.completion_failure_reply,'presentation':result.response_presentation,
        'review':str(result.outcome_review_receipt),'steps':model.steps,'reviews':model.reviews,
        'tools':[(r.tool_name,r.status,r.error) for r in result.tool_results]},ensure_ascii=False)
    assert model.reviews == 0 and len(f.readings) == 1
    assert [r.tool_name for r in result.tool_results]==['sensor_read','create_live_tracker']
    assert len(f.store.list('workspace_admin'))==1
    receipts=[r.output['deferred_execution'] for r in result.tool_results if isinstance(r.output,dict) and 'deferred_execution' in r.output]
    assert receipts[0]['handle']==receipts[-1]['handle']
    assert receipts[-1]['configuration']['duration_seconds']==10800
    assert 'Some requested search filters remain unverified' not in result.final_text

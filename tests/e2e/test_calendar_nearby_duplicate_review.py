"""Real adapter/runner/action review, with external provider boundaries faked."""
import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker workflow boundary')


@pytest.mark.parametrize('title',['同じ義務の通知','Aviso de la misma obligación'])
@pytest.mark.parametrize('paged',[False,True])
def test_existing_reminder_on_nearby_date_is_visible_before_create(tmp_path,monkeypatch,title,paged):
    from nullion import tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    runtime=setup_runtime(tmp_path,monkeypatch);registry=tools.ToolRegistry();tools.register_connector_plugin(registry)
    for kind in [BoundaryKind.ACCOUNT_ACCESS,BoundaryKind.OUTBOUND_NETWORK]:
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id=kind.value,principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind,mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    monkeypatch.setattr(tools,'_connector_connection_for_invocation',lambda *a:SimpleNamespace())
    monkeypatch.setattr(tools,'_calendar_events_endpoint_for_provider',lambda *a,**kw:'https://fixture.invalid/events')
    rows=[{'id':str(i),'summary':'別の予定','start':{'date':'2028-12-01'}} for i in range(3)]
    rows.append({'id':'existing','summary':title,'start':{'date':'2028-11-16'}})
    requests=[]
    def network(invocation,**kw):
        requests.append(kw)
        assert kw.get('method','GET')=='GET','A duplicate must not reach the external mutation'
        if paged:
            if kw['params'].get('pageToken'):
                assert kw['params']['pageToken']=='opaque-next'
                return {'items':rows[3:]}
            return {'items':rows[:3],'nextPageToken':'opaque-next'}
        return {'items':rows}
    monkeypatch.setattr(tools,'_connector_json_request',network)
    class Provider:
        rounds=0;action_reviews=0
        def create(self,**kw):
            system=kw.get('system') or ''
            if system.startswith('Workflow action preflight'):
                self.action_reviews+=1
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                context=packet['adapter_target_evidence']['context'];observed=context['observed_events']
                if paged:
                    assert context['listing_scopes'][0]['has_more'] is True
                    assert context['listing_scopes'][0]['next_page_token']=='opaque-next'
                else:
                    assert observed[0]['id']=='existing' and observed[0]['summary']==title
                return text_response(json.dumps({'decision':'revise','issue_kind':'concrete_conflict','step_effect':'mutation',
                    'issues':['Inspect the remaining calendar page before creating a reminder.' if paged
                        else 'The same obligation already has a reminder on the nearby date. Retain it.']}))
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'Report the existing matching reminder','requirement_kind':'fact',
                        'evidence_subject_id':'existing','eligible_evidence_tool_names':['calendar_list']}]}))
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            self.rounds+=1
            if self.rounds==1:return tool_response('calendar_list',{'provider_id':'fixture',
                'start':'2028-11-15T00:00:00Z','end':'2028-12-03T00:00:00Z','max':50})
            if self.rounds==2 and paged:
                assert 'opaque-next' in json.dumps(kw['messages'])
                return tool_response('calendar_list',{'provider_id':'fixture','start':'2028-11-15T00:00:00Z',
                    'end':'2028-12-03T00:00:00Z','max':50,'page_token':'opaque-next'})
            return text_response('Existing reminder retained: '+title)
    provider=Provider()
    result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:calendar',principal_id='web:calendar',
        user_message='予定表を確認し、同じ義務の通知を重複させないでください。',conversation_history=[],tool_registry=registry,
        policy_store=runtime.store,approval_store=runtime.store,max_iterations=5)
    assert provider.action_reviews==0 and len(requests)==(2 if paged else 1)
    assert 'Existing reminder retained' in result.final_text
    events=runtime.store.list_recent_conversation_events('web:calendar',event_type='conversation.action_review')
    assert not events

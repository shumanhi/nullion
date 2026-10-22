"""Collection and detail scopes through real API, persistence, and delivery."""
from __future__ import annotations
import json
import os
import time
import pytest
pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='container E2E only')

@pytest.mark.parametrize('prompt,detail', [('Check my email',False),('Revisa mi bandeja',False),('受信箱を確認して',False),('Open the second message',True)])
def test_optional_detail_visibility_does_not_replace_collection_answer(tmp_path, monkeypatch, prompt, detail):
    from fastapi.testclient import TestClient
    from nullion import web_app, turn_context_policy
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setattr(turn_context_policy,'_active_connector_provider_context',lambda:[{
        'provider_id':'custom_connector_provider','display_name':'Example connector',
        'active_app_ids':['google-mail'],'structured_tools':['email_search','email_read']}])
    calls=[];registry=ToolRegistry()
    def external_mail(inv):
        calls.append(inv.tool_name)
        output={'results':[{'id':'message-one','subject':'First notice','from':'one@example.com','snippet':'First update'},
                           {'id':'message-two','subject':'Second notice','from':'two@example.com','snippet':'Second update'}],
                'body_included':False,'body_requires_tool':'email_read','next_tool_for_body':'email_read'}
        if inv.tool_name=='email_read':
            output={'message':{'id':'message-two','subject':'Second notice','body':'The second message has the requested detail.'}}
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',output)
    for name in ('email_search','email_read'):
        registry.register(ToolSpec(name=name,description=name,risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=2,
            capability_tags=('email','connector','account_read'),
            input_schema={'type':'object','properties':{},'additionalProperties':True}),external_mail)
    required=['email_search','email_read'] if detail else ['email_search']
    class Model:
        def create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            if not names:
                return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps({
                    'skill_pack_action':'connector','connector_app_ids':['google-mail'],
                    'connector_source_user_requested':True,'connector_source_evidence':prompt,
                    'requested_tool_names':['email_search','email_read'],'required_tool_names':required,'confidence':1})}]}
            if 'email_search' not in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope','input':{
                    'capabilities':['connector'],'source_user_requested':True,'source_evidence':prompt,
                    'connector_app_ids':['google-mail'],'tool_names':['email_search','email_read'],'required_tool_names':required}}]}
            if not calls:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'search','name':'email_search','input':{'query':'in:inbox'}}]}
            if detail and 'email_read' not in calls:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'email_read','input':{'id':'message-two'}}]}
            text='Second notice: The second message has the requested detail.' if detail else 'Two inbox messages: First notice — First update; Second notice — Second update.'
            return {'stop_reason':'end_turn','content':[{'type':'text','text':text}]}
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Model();runtime.model_client=model
    from datetime import UTC, datetime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    for name in ('email_search','email_read'):
        runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission='tool:'+name,granted_by='fixture',granted_at=datetime.now(UTC)))
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('mail-fixture',OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.ACCOUNT_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    runtime.checkpoint()
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
    with TestClient(app) as client:
        before=time.perf_counter();response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:collection'})
        elapsed=time.perf_counter()-before
    assert response.status_code==200,response.text
    payload=response.json();assert payload.get('artifacts',[])==[],payload
    assert 'Second notice' in payload['text'],payload
    assert ('First notice' in payload['text']) is (not detail),payload
    assert calls==(['email_search','email_read'] if detail else ['email_search']),calls
    events=runtime.store.list_conversation_events('web:collection')
    turn=next(x for x in reversed(events) if x.get('event_type')=='conversation.chat_turn')
    assert [t['tool_name'] for t in turn['tool_results'] if t['tool_name']!='request_tool_scope']==calls
    assert elapsed<3,elapsed

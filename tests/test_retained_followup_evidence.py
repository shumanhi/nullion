"""Completed read evidence is reusable only through typed selected lineage."""
from datetime import datetime, timezone
from types import SimpleNamespace
import json

import pytest

from nullion.conversation_runtime import ConversationTurn, ConversationTurnDisposition as Disposition
from nullion.connections import workspace_id_for_principal
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
from nullion.turn_context_policy import (TurnToolScopeDecision, TurnToolEvidence, ScopedTurnToolRegistry,
    _parse_turn_tool_scope_decision, _tool_scope_decision_to_payload, _tool_scope_decision_from_payload)


@pytest.mark.parametrize('policy',['fresh','retained'])
def test_observation_policy_round_trips_without_text_classification(policy):
    decision=_parse_turn_tool_scope_decision('{"evidence_policy":"'+policy+'","confidence":1}')
    assert decision.valid and decision.evidence_policy==policy
    assert _tool_scope_decision_from_payload(_tool_scope_decision_to_payload(decision))==decision
    assert _parse_turn_tool_scope_decision('{}').evidence_policy=='fresh'
    assert not _parse_turn_tool_scope_decision('{"evidence_policy":"guess"}').valid


@pytest.mark.parametrize('value', [None, True, 1, [], {}])
def test_malformed_observation_policy_is_invalid_instead_of_crashing(value):
    assert not _parse_turn_tool_scope_decision(json.dumps({'evidence_policy': value})).valid
    cached = _tool_scope_decision_to_payload(TurnToolScopeDecision(valid=True))
    cached['evidence_policy'] = value
    assert _tool_scope_decision_from_payload(cached) is None


@pytest.mark.parametrize('reason,parents,expected_empty',[
    ('no_active_turn',(),True),('model_structured_separate',(),False),
    ('model_structured_revision',('selected',),False)])
def test_web_and_common_dispatch_use_the_same_typed_kernel_contract(reason,parents,expected_empty):
    from nullion.web_app import _web_dispatch_kwargs
    from nullion.chat_operator import _conversation_dispatch_kwargs
    decision=SimpleNamespace(reason=reason,dependency_turn_ids=parents,
        disposition=Disposition.REVISE if parents else Disposition.INDEPENDENT)
    actual=_web_dispatch_kwargs(decision)
    assert actual==_conversation_dispatch_kwargs(decision)
    assert (actual=={}) is expected_empty


def test_retained_followup_does_not_get_a_new_live_read_instruction():
    from nullion.chat_operator import _apply_structured_tool_context_scope
    registry=SimpleNamespace(turn_tool_scope_decision=TurnToolScopeDecision(valid=True,evidence_policy='retained'))
    assert _apply_structured_tool_context_scope(registry,base_registry=None,evidence=None,
        context='opaque source metadata')==(registry,(),None)


def test_only_verified_parent_receipt_availability_wakes_the_scope_model():
    from nullion.turn_context_policy import turn_tool_evidence_needs_model_scope_decision
    assert not turn_tool_evidence_needs_model_scope_decision(TurnToolEvidence(
        context_linked=True,prior_tool_scopes=('web',)))
    assert turn_tool_evidence_needs_model_scope_decision(TurnToolEvidence(
        context_linked=True,prior_tool_scopes=('web',),retained_read_available=True))


def test_scope_cache_changes_when_verified_parent_evidence_becomes_available():
    from nullion.turn_context_policy import _tool_scope_cache_key
    keys = [_tool_scope_cache_key(user_message='資料を保持。',
        evidence=TurnToolEvidence(context_linked=True, prior_tool_scopes=('web',),
            retained_read_available=available), registry=ToolRegistry(), model_client=None)
        for available in (False, True)]
    assert keys[0] != keys[1]
    assert keys[1]['evidence']['retained_read_available'] is True


def test_scope_output_budget_can_complete_the_structured_decision(monkeypatch):
    from nullion.turn_context_policy import build_turn_tool_scope_decision
    monkeypatch.delenv('NULLION_TOOL_SCOPE_CLASSIFIER_MAX_TOKENS', raising=False)
    base=ToolRegistry()
    base.register(ToolSpec('browser_extract_text','Read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),lambda _:None)
    class Provider:
        def create(self, **kwargs):
            # Model boundary: the full decision must fit, including reasoning.
            if kwargs['max_tokens'] < 300:
                return {'content':[{'type':'text','text':'{"requested_outcome":"unspecified",'}],
                    'stop_reason':'max_tokens'}
            return {'content':[{'type':'text','text':json.dumps({'evidence_policy':'retained',
                'web_action':'none','requested_tool_names':[],'required_tool_names':[],
                'requested_artifact_extensions':[],'required_embedded_media_extensions':[],
                'confidence':1})}],'stop_reason':'end_turn'}
    decision=build_turn_tool_scope_decision(model_client=Provider(),user_message='資料を保持。',
        evidence=TurnToolEvidence(context_linked=True,retained_read_available=True,prior_tool_scopes=('web',)),
        registry=base,cache_decision=False)
    assert decision.valid and decision.evidence_policy=='retained'


@pytest.mark.parametrize('valid', [False, True])
@pytest.mark.parametrize('prompt', ['保持した情報だけを説明して。', 'Explica solo los datos retenidos.'])
def test_common_schema_recovery_does_not_reopen_an_external_read(prompt,valid):
    from nullion.chat_operator import _run_chat_turn_common_retries
    base=ToolRegistry()
    base.register(ToolSpec('browser_navigate','Source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3),
        lambda inv: (_ for _ in ()).throw(AssertionError('Source reopened')))
    evidence=TurnToolEvidence(context_linked=True,prior_tool_scopes=('web',),retained_read_available=True)
    registry=ScopedTurnToolRegistry(base,evidence=evidence,tool_scope_decision=TurnToolScopeDecision(
        valid=valid,evidence_policy='retained' if valid else 'fresh',requested_tool_names=('browser_navigate',)))
    result=SimpleNamespace(final_text='Recorded observation: 53.',tool_results=[],artifacts=[],suspended_for_approval=False)
    recovered=_run_chat_turn_common_retries(initial_result=result,base_history=[],active_tool_registry=registry,
        base_tool_registry=base,evidence=evidence,user_message=prompt,fallback_tool_names=('browser_navigate',),
        run_turn=lambda *_: (_ for _ in ()).throw(AssertionError('Unrequested source retry')))
    assert recovered.result is result and not recovered.retried_schema_followup


@pytest.mark.parametrize('identity_matches', [True, False])
def test_retry_preserves_only_request_owned_observation_timestamps(identity_matches):
    from nullion.agent_orchestrator import TurnContinuationEvidence
    from nullion.chat_operator import _RevisionContinuationRegistry, _registry_with_request_retry_evidence
    from nullion.tools import ToolResult
    receipt = ToolResult('continuation:parent:0', 'browser_extract_text', 'completed', {'text': '53'})
    records = {receipt.invocation_id: {'observed_at': '2028-01-02T10:00:00Z'},
               'absent': {'observed_at': '2028-01-01T10:00:00Z'}}
    context = TurnContinuationEvidence('web:current' if identity_matches else 'web:other',
        'web:current', ('parent',), (receipt,),
        retained_read_context={'evidence_policy': 'retained', 'records': records})
    registry = _RevisionContinuationRegistry(SimpleNamespace(), context)
    result = SimpleNamespace(turn_id='current', tool_results=(receipt,))
    wrapped = _registry_with_request_retry_evidence(registry, result=result,
        conversation_id='web:current', principal_id='web:current', user_message='資料を保持。')
    actual = wrapped.turn_continuation_evidence.retained_read_context
    assert actual == ({'evidence_policy': 'retained', 'records': {
        receipt.invocation_id: records[receipt.invocation_id]}} if identity_matches else {})
    if identity_matches:
        actual['records'][receipt.invocation_id]['observed_at'] = 'changed'
        assert records[receipt.invocation_id]['observed_at'] == '2028-01-02T10:00:00Z'


@pytest.mark.parametrize('name,tags',[('browser_navigate',()),('browser_extract_text',()),
    ('weather_forecast',()),('market_quote',()),('email_read',()),('calendar_list',()),
    ('opaque',('connector',)),('opaque',('forecast',))])
def test_retained_policy_hides_external_reads_even_if_requested(name,tags):
    base=ToolRegistry()
    base.register(ToolSpec(name,'External read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3,
        capability_tags=tags),lambda invocation:(_ for _ in ()).throw(AssertionError('Unexpected external read')))
    view=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(context_linked=True,has_url_target=True,
        prior_tool_scopes=('web','connector')),tool_scope_decision=TurnToolScopeDecision(evidence_policy='retained',
        valid=True,web_action='open_url',requested_tool_names=(name,),required_tool_names=(name,)))
    assert not view.can_invoke_tool(name)
    assert name not in {r['name'] for r in view.list_tool_definitions()}


@pytest.mark.parametrize('disposition',[Disposition.CONTINUE,Disposition.REVISE,Disposition.BACKGROUND_FOLLOW_UP])
def test_completed_selected_parent_supplies_exact_reads_without_a_permission_grant(tmp_path,disposition):
    from nullion.chat_operator import _registry_with_inflight_revision_evidence
    from nullion.agent_orchestrator import _continuation_model_evidence_message
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:retained';base=ToolRegistry()
    for name,effect in [('read',ToolSideEffectClass.READ),('write',ToolSideEffectClass.WRITE),('request_tool_scope',ToolSideEffectClass.READ)]:
        base.register(ToolSpec(name,'Fixture',ToolRiskLevel.LOW,effect,False,3),lambda inv:None)
    parent=ConversationTurn('parent',cid,'branch',None,Disposition.INDEPENDENT,'α','completed',datetime.now(timezone.utc))
    runtime.store.add_conversation_turn(parent)
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':cid,
        'turn_id':'parent','workspace_id':workspace_id_for_principal(cid),'created_at':'2028-01-02T10:00:00Z',
        'tool_results':[{'invocation_id':'r','tool_name':'read','status':'completed','output':{'text':'資料 53'}},
            {'tool_name':'write','status':'completed','output':{'sent':True}},
            {'tool_name':'request_tool_scope','status':'completed','output':{'tools':['write']}},
            {'tool_name':'read','status':'failed','output':{}},
            {'tool_name':'read','status':'completed','error':'not reliable','output':{'text':'wrong'}}]})
    registry=SimpleNamespace(turn_tool_scope_decision=TurnToolScopeDecision(valid=True,evidence_policy='retained'))
    linked=SimpleNamespace(turn=SimpleNamespace(conversation_id=cid,parent_turn_id='parent',disposition=disposition))
    wrapped=_registry_with_inflight_revision_evidence(registry,runtime=runtime,conversation_id=cid,principal_id=cid,
        decision=None,base_registry=base,conversation_result=linked)
    evidence=wrapped.turn_continuation_evidence
    assert len(evidence.tool_results)==1 and evidence.tool_results[0].output=={'text':'資料 53'}
    assert not evidence.same_request_retry and not evidence.completed_action_results
    assert evidence.retained_read_context['records']['continuation:parent:0']['observed_at']=='2028-01-02T10:00:00Z'
    text=str(_continuation_model_evidence_message(evidence.tool_results,provenance=evidence.retained_read_context))
    assert '資料 53' in text and '2028-01-02T10:00:00Z' in text


@pytest.mark.parametrize('case',['fresh','independent','other_conversation','missing_parent','reset','workspace'])
def test_completed_history_never_crosses_its_selected_scope(tmp_path,case):
    from nullion.chat_operator import _registry_with_inflight_revision_evidence
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:retained';base=ToolRegistry()
    base.register(ToolSpec('read','Read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3),lambda inv:None)
    runtime.store.add_conversation_turn(ConversationTurn('parent',cid,'branch',None,
        Disposition.INDEPENDENT,'α','completed',datetime.now(timezone.utc)))
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':cid,'turn_id':'parent',
        'workspace_id':'other' if case=='workspace' else workspace_id_for_principal(cid),
        'tool_results':[{'tool_name':'read','status':'completed','output':{'text':'53'}}]})
    if case=='reset':runtime.store.add_conversation_event({'event_type':'conversation.session_reset','conversation_id':cid})
    registry=SimpleNamespace(turn_tool_scope_decision=TurnToolScopeDecision(valid=True,
        evidence_policy='fresh' if case=='fresh' else 'retained'))
    linked=SimpleNamespace(turn=SimpleNamespace(conversation_id='other' if case=='other_conversation' else cid,
        parent_turn_id=None if case=='missing_parent' else 'parent',
        disposition=Disposition.INDEPENDENT if case=='independent' else Disposition.CONTINUE))
    assert _registry_with_inflight_revision_evidence(registry,runtime=runtime,conversation_id=cid,principal_id=cid,
        decision=None,base_registry=base,conversation_result=linked) is registry

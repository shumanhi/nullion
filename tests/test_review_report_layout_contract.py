import json
from copy import deepcopy

import pytest
from jsonschema import validate, ValidationError

from nullion.agent_orchestrator import (_CompletionRequirementContractRow as Contract,
    _completion_review_response_schema, _completion_review_request_context,
    _parse_completion_review_decision)
from nullion.chat_text import REPORT_MESSAGE_FORMAT, render_report_message
from nullion.preferences import Preferences
from tests.test_reviewed_presentation_correction import correction_payload


@pytest.mark.parametrize('kind', [None, 'finding', 'limitation'])
@pytest.mark.parametrize('citations', [None, ['source']])
def test_partial_report_schema_requires_common_layout_without_losing_citation_rules(kind, citations):
    payload = correction_payload(); payload.pop('reply_target')
    payload.update(disposition='blocked', unresolved_requirements=['One requested result remains unconfirmed.'],
        user_facing_failure='Private diagnostic')
    payload['material_requirements'][0].update(satisfied=False, draft_covered=False)
    row=payload['supported_findings'][0]
    if kind: row['kind']=kind
    schema=_completion_review_response_schema((Contract('r','対象','fact',('read',)),),
        citable_evidence_ids=citations, structured_findings=True)
    validate(payload,schema)
    legacy=deepcopy(payload); legacy['supported_findings'][0]['text']='A: 7. B: 8.'
    if kind=='limitation': validate(legacy,schema)
    else:
        with pytest.raises(ValidationError): validate(legacy,schema)
    if citations:
        bad=deepcopy(payload);bad['supported_findings'][0]['evidence_invocation_ids']=['not-current']
        with pytest.raises(ValidationError): validate(bad,schema)


def test_legacy_compatibility_preserves_public_facts_without_format_retry_or_completion_change():
    payload=correction_payload();payload.pop('reply_target')
    payload.update(disposition='blocked',unresolved_requirements=['A requested result remains missing.'])
    payload['supported_findings'][0]['text']='Un párrafo literal; 1. literal. 2. literal.'
    review=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'read'},requirement_contract=(Contract('r','対象','fact',('read',)),))
    assert review.disposition=='blocked' and review.review_error is None
    assert review.supported_findings[0].text==payload['supported_findings'][0]['text']


def test_draft_expansion_cannot_override_saved_detail_policy_or_invalidate_review_context(monkeypatch):
    from nullion import preferences
    monkeypatch.setattr(preferences,'load_preferences',lambda:Preferences(response_length='balanced'))
    base={'response_presentation':{'report_presentation':'structured','report_detail_level':'detailed'}}
    context=_completion_review_request_context(base)
    assert context['report_presentation_contract']['detail_level']=='compact'
    compact={'response_presentation':{'report_presentation':'structured','report_detail_level':'compact'}}
    assert _completion_review_request_context(compact)==context
    monkeypatch.setattr(preferences,'load_preferences',lambda:Preferences(response_length='detailed'))
    assert _completion_review_request_context(base)['report_presentation_contract']['detail_level']=='detailed'


@pytest.mark.parametrize('emoji', ['none','minimal','expressive'])
def test_report_and_fallback_share_emoji_policy_and_preserve_required_content(emoji):
    message=correction_payload()['supported_findings'][0]['text']['message']
    result=render_report_message(message,preferences=Preferences(emoji_level=emoji))
    assert result.count('📊')==(0 if emoji=='none' else 1)
    assert '$720' in result and '$760' in result and 'Historical detail' not in result


@pytest.mark.parametrize('user', ['木材向けの強い接着剤で価格を比較。', 'Compara adhesivos resistentes para madera por precio.'])
def test_reply_reference_keeps_owned_user_goal_instead_of_only_the_assistants_catalog(user):
    state={'tool_flow_context':{'request_context':{'parent_turn_id':'owned-parent',
        'quoted_message':{'reply_to_from_bot':True,'reply_to_text':'Earlier assistant catalog and optional specifications.'},
        'linked_turns':[{'user':user,'assistant':'Earlier assistant catalog and optional specifications.'}]}}}
    result=_completion_review_request_context(state)['request_context']
    assert result['quoted_reference_role']=='assistant'
    assert result['parent_turn_id']=='owned-parent' and result['linked_turns'][0]['user']==user
    assert result['quoted_message']['reply_to_text'].startswith('Earlier assistant')
    state['tool_flow_context']['request_context'].pop('parent_turn_id')
    assert 'linked_turns' not in _completion_review_request_context(state)['request_context']


def test_corrected_report_need_not_narrate_a_verified_internal_prerequisite():
    payload=correction_payload()
    payload['material_requirements'].append({'requirement_id':'scope','satisfied':True,
        'draft_covered':False,'evidence_invocation_ids':['source']})
    contract=(Contract('r','対象','fact',('read',)),
        Contract('scope','実行条件','constraint',('read',),delivery_required=False))
    result=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'read'},requirement_contract=contract)
    assert result is not None and result.corrected_reply
    assert '実行条件' not in result.corrected_reply
    payload['material_requirements'][1]['satisfied']=False
    assert _parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'read'},requirement_contract=contract) is None

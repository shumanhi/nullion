"""Native mechanical validation must not become a model veto or consent grant."""
import json

import pytest

from nullion.tools import ToolInvocation, ToolSpec, ToolRiskLevel, ToolSideEffectClass
from nullion.workflow_reasoning import assess_action


@pytest.mark.parametrize('scopes,validated,decision', [
    (['native_contract'], True, 'proceed'),
    (['tool_registration'], False, 'proceed'),
    (['tool_registration', 'explicit_constraint'], True, 'revise'),
    (['declared_default'], True, 'proceed'),
    (['native_contract', 'declared_default'], True, 'proceed'),
    (['native_contract', 'explicit_constraint'], True, 'revise'),
    (['request_intent'], True, 'revise'),
    (['target'], True, 'revise'),
    (['unknown'], True, 'revise'),
    ([], True, 'revise'),
    (['native_contract'], False, 'revise'),
    (['declared_default'], False, 'revise'),
])
def test_native_facts_only_adjudicate_mechanical_concerns(scopes, validated, decision):
    calls = []
    def context(inv, retained):
        return {'native_contract_validation': {'validated': validated,
            'advisory_concern_scopes': ['native_contract', 'declared_default']}}
    spec = ToolSpec('opaque_accept', 'Accept selected local configuration', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2, action_review_context=context)
    class Provider:
        def create(self, **kw):
            calls.append(kw)
            return {'content': [{'type': 'text', 'text': json.dumps({'decision': 'revise',
                'issues': ['別の識別子が必要です。'], 'concern_scopes': scopes})}]}
    result = assess_action(model_client=Provider(), request='選択した条件で開始してください。', context={},
        invocation=ToolInvocation('accept', spec.name, 'web:one', {}), spec=spec,
        evidence=[], now='2028-04-02T12:00:00Z')
    assert not result.error and result.decision == decision
    assert len(calls) == 1
    assert result.issues == ('別の識別子が必要です。',)  # Keep diagnosis, not user-facing instructions.
    assert result.advisory is (decision == 'proceed')


def test_provider_cannot_supply_native_authority():
    spec = ToolSpec('opaque_send', 'Send selected notice', ToolRiskLevel.HIGH,
        ToolSideEffectClass.WRITE, True, 2)
    class Provider:
        def create(self, **kw):
            return {'content': [{'type': 'text', 'text': json.dumps({'decision': 'revise',
                'issues': ['Missing native contract'], 'concern_scopes': ['native_contract']})}]}
    result = assess_action(model_client=Provider(), request='Préparez le message.', context={},
        invocation=ToolInvocation('send', spec.name, 'web:one', {}), spec=spec,
        evidence=[], now='2028-04-02T12:00:00Z')
    assert not result.error and result.decision == 'revise' and not result.advisory


def test_native_facts_do_not_override_a_question_or_transport_failure():
    spec = ToolSpec('opaque_accept', 'Accept selected configuration', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2, action_review_context=lambda *args: {
            'native_contract_validation': {'validated': True, 'advisory_concern_scopes': ['native_contract']}})
    class Provider:
        def create(self, **kw):
            return {'content': [{'type': 'text', 'text': json.dumps({'decision': 'clarify',
                'issues': ['Two destinations'], 'concern_scopes': ['native_contract'],
                'question': '¿Cuál?', 'options': ['Primero', 'Segundo']})}]}
    result = assess_action(model_client=Provider(), request='Iniciar.', context={},
        invocation=ToolInvocation('accept', spec.name, 'web:one', {}), spec=spec,
        evidence=[], now='2028-04-02T12:00:00Z')
    assert result.decision == 'clarify' and not result.error and not result.advisory

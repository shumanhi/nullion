"""Retired quality protocols do not run, retry, inspect, or grant consent."""
from types import SimpleNamespace
import pytest
from nullion.workflow_reasoning import assess_action


@pytest.mark.parametrize('protocol', ['invalid_json', 'revise', 'clarify', 'timeout', 'inspect_evidence', 'complete'])
@pytest.mark.parametrize('prompt', ['対象を変更。', 'Modifica el destino.'])
def test_retired_action_protocol_has_no_model_or_approval_authority(protocol, prompt):
    class Forbidden:
        def create(self, **kwargs):
            pytest.fail('A retired quality protocol must never contact the provider')
    invocation = SimpleNamespace(invocation_id='owned', tool_name='resource_write', principal_id='member')
    result = assess_action(model_client=Forbidden(), request=prompt, context={'legacy_protocol':protocol},
        invocation=invocation, spec=None, evidence=[], now='2026-11-12')
    assert result.decision == 'proceed' and not result.issues and not result.options
    assert result.question is None and result.error is None
    assert not hasattr(result, 'approval_granted')

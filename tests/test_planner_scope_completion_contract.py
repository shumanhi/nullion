import pytest
from nullion.tools import ToolRegistry
from nullion.turn_context_policy import TurnToolEvidence,TurnToolScopeDecision,_planner_materialized_registry
from nullion.agent_orchestrator import _required_attachment_extensions_from_turn_state


@pytest.mark.parametrize('formats',[(),('.html',),('.pdf',),('.png','.html')])
def test_planner_tool_availability_does_not_add_required_formats_or_invent_source_signals(formats):
    evidence=TurnToolEvidence(requested_extensions=formats)
    registry=_planner_materialized_registry(ToolRegistry(),evidence=evidence,
        decision=TurnToolScopeDecision(requested_artifact_extensions=formats,valid=True,confidence=1))
    decision=registry.turn_tool_scope_decision
    assert decision.requested_artifact_extensions==formats
    assert _required_attachment_extensions_from_turn_state({'tool_registry':registry})==formats
    assert registry._evidence.has_url_target is False and registry._evidence.has_attachments is False
    assert decision.web_action=='none'


def test_planner_keeps_source_file_exclusions_and_explicit_existing_file_delivery():
    registry=_planner_materialized_registry(ToolRegistry(),evidence=TurnToolEvidence(requested_extensions=('.zip','.html')),
        decision=TurnToolScopeDecision(requested_artifact_extensions=('.html',),excluded_artifact_extensions=('.zip',),
            source_artifact_delivery_requested=True,source_artifact_request_evidence='attachment-7',valid=True,confidence=1))
    decision=registry.turn_tool_scope_decision
    assert decision.requested_artifact_extensions==('.html',) and decision.excluded_artifact_extensions==('.zip',)
    assert decision.source_artifact_delivery_requested and decision.source_artifact_request_evidence=='attachment-7'

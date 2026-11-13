import pytest
from nullion.agent_orchestrator import _completion_review_request_timeout

@pytest.mark.parametrize('tokens,expected',[(1100,20),(3200,45),(8192,60)])
def test_review_budget_scales_only_for_actual_structured_response(monkeypatch,tokens,expected):
    monkeypatch.delenv('NULLION_COMPLETION_REVIEW_TIMEOUT_SECONDS',raising=False)
    assert _completion_review_request_timeout(tokens)==expected

@pytest.mark.parametrize('configured,expected',[('3',3),('45',45),('90',60)])
def test_explicit_review_deadline_remains_authoritative(monkeypatch,configured,expected):
    monkeypatch.setenv('NULLION_COMPLETION_REVIEW_TIMEOUT_SECONDS',configured)
    assert _completion_review_request_timeout(8192)==expected


@pytest.mark.parametrize('tokens,expected',[(1100,20),(2000,30),(3200,45),(8192,60)])
def test_requirement_planning_uses_its_structured_output_budget(monkeypatch,tokens,expected):
    from nullion.agent_orchestrator import _completion_requirement_timeout_seconds
    monkeypatch.delenv('NULLION_COMPLETION_REQUIREMENT_TIMEOUT_SECONDS',raising=False)
    assert _completion_requirement_timeout_seconds(tokens)==expected

@pytest.mark.parametrize('configured,expected',[('2',2),('45',45),('90',60)])
def test_explicit_requirement_deadline_is_preserved(monkeypatch,configured,expected):
    from nullion.agent_orchestrator import _completion_requirement_timeout_seconds
    monkeypatch.setenv('NULLION_COMPLETION_REQUIREMENT_TIMEOUT_SECONDS',configured)
    assert _completion_requirement_timeout_seconds(8192)==expected





@pytest.mark.parametrize("contradicted,covered,valid", [(False, False, True), (True, False, True),
    (True, True, False), ("false", False, False)])
def test_review_distinguishes_missing_coverage_from_contradicted_findings(contradicted, covered, valid):
    import json
    from nullion.agent_orchestrator import (_parse_completion_review_decision,
        _CompletionRequirementContractRow)
    contract = _CompletionRequirementContractRow("r1", "Report the selected value", "fact", ("opaque_read",))
    packet = {"disposition": "retry", "all_material_requirements_enumerated": True,
        "unresolved_requirements": ["Correct the selected report"], "retry_tool_names": [],
        "material_requirements": [{"requirement_id": "r1", "satisfied": True,
            "draft_covered": covered, "draft_contradicted": contradicted,
            "evidence_invocation_ids": ["read"]}]}
    decision = _parse_completion_review_decision(
        {"content": [{"type": "text", "text": json.dumps(packet)}]},
        registered_tool_names={"opaque_read"}, requirement_contract=[contract])
    assert (decision is not None) is valid
    if valid:
        assert decision.material_requirements[0].draft_contradicted is contradicted

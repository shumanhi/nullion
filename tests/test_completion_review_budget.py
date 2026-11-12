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


def test_prepared_file_versions_do_not_reenumerate_unchanged_material_contract(monkeypatch):
    import json
    from types import SimpleNamespace
    from nullion import agent_orchestrator as agent
    from nullion.tools import ToolRegistry, ToolResult
    calls=[]
    class Model:
        def create(self,**kwargs):
            calls.append(kwargs)
            return {'content':[{'type':'text','text':json.dumps({'all_material_requirements_enumerated':True,
                'material_requirements':[{'requirement':'指定されたCSVを作成する。','requirement_kind':'deliverable',
                    'evidence_subject_id':'csv','eligible_evidence_tool_names':['file_write']}]})}]}
    monkeypatch.setattr(agent,'_turn_has_artifact_delivery_contract',lambda state:True)
    state={'orchestrator':SimpleNamespace(model_client=Model()),'tool_registry':ToolRegistry(),
        'user_message':'指定されたCSVを作成する。','selected_artifact_paths':['/workspace/first.csv'],
        'tool_flow_context':{'artifact_extensions':['.csv']}}
    results=[ToolResult('write','file_write','completed',{'path':'/workspace/first.csv'})]
    first=agent._extract_completion_requirement_contract(state,tool_results=results)
    assert first and len(calls)==1
    state['selected_artifact_paths']=['/workspace/repaired.csv']
    assert agent._extract_completion_requirement_contract(state,tool_results=results)==first
    assert len(calls)==1
    state['user_message']='別のCSVを作成する。'
    assert agent._extract_completion_requirement_contract(state,tool_results=results)
    assert len(calls)==2

@pytest.mark.parametrize('prompt',['Describe the saved result.','Describe el resultado guardado.','保存した結果を説明して。'])
def test_invalid_requirement_protocol_is_bounded_and_records_schema_reason(prompt):
    from types import SimpleNamespace
    from nullion import agent_orchestrator as agent
    from nullion.tools import ToolRegistry, ToolResult
    calls=[]; events=[]
    class Model:
        def create(self,**kwargs):
            import json
            payload=json.loads(kwargs['messages'][0]['content'][0]['text']); calls.append(payload)
            if len(calls)==2:
                assert payload['runtime_requirement_validation_errors']==['missing_requirements']
            assert kwargs['tools']==[]
            return {'content':[{'type':'text','text':'{"all_material_requirements_enumerated":true,"material_requirements":[]}'}]}
    state={'orchestrator':SimpleNamespace(model_client=Model()),'tool_registry':ToolRegistry(),
        'runtime_store':SimpleNamespace(add_conversation_event=events.append),'user_message':prompt}
    assert agent._extract_completion_requirement_contract(state,
        tool_results=[ToolResult('read','file_read','completed',{'content':'verified result'})])==()
    assert len(calls)==2
    assert [event['attempt'] for event in events]==[1,2]
    assert all(event['status']=='invalid' and event['validation_errors']==['missing_requirements'] for event in events)


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

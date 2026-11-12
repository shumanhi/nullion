import hashlib
from types import SimpleNamespace

from nullion.artifact_evidence import artifact_review_records
from nullion.tools import ToolResult,ToolSpec,ToolRegistry,ToolRiskLevel,ToolSideEffectClass


def setup_outputs(tmp_path):
    paths=[tmp_path/'α.csv',tmp_path/'β.html']
    paths[0].write_text('Name,Value\nNorth,12\n');paths[1].write_text('<html><body>South 19</body></html>')
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('generate','Generate',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),lambda inv:None)
    registry.register(ToolSpec('read','Read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),lambda inv:None)
    result=ToolResult('one-generator','generate','completed',{'artifact_paths':[str(path) for path in paths]})
    return paths,registry,result


def test_artifact_records_bind_distinct_files_to_one_real_producer(tmp_path):
    paths,registry,result=setup_outputs(tmp_path)
    # A focused repair view must not erase metadata of completed producers.
    from nullion.agent_orchestrator import _FocusedRecoveryToolRegistry
    records,fingerprints=artifact_review_records(paths,[result],registry=_FocusedRecoveryToolRegistry(registry,set()),allowed_roots=[tmp_path])
    assert len(records)==2 and len({record.invocation_id for record in records})==2
    for path,record in zip(paths,records,strict=True):
        binding=record.output['artifact_record_binding']
        assert binding['sha256']==hashlib.sha256(path.read_bytes()).hexdigest()
        assert binding['parent_invocation_id']==result.invocation_id and binding['filename']==path.name
        assert record.invocation_id in fingerprints
    assert len(set(fingerprints.values()))==2


def test_read_unreceipted_or_outside_files_cannot_become_delivery_proof(tmp_path):
    paths,registry,result=setup_outputs(tmp_path)
    read=ToolResult('read','read','completed',result.output)
    assert artifact_review_records(paths,[read],registry=registry,allowed_roots=[tmp_path])==([],{})
    assert artifact_review_records(paths,[],registry=registry,allowed_roots=[tmp_path])==([],{})
    assert artifact_review_records(paths,[result],registry=registry,allowed_roots=[tmp_path/'different'])==([],{})


def test_bound_files_prove_only_their_own_deliverables_and_not_live_facts(tmp_path):
    import nullion.agent_orchestrator as agent
    paths,registry,result=setup_outputs(tmp_path)
    records,fingerprints=artifact_review_records(paths,[result],registry=registry,allowed_roots=[tmp_path])
    contract=[agent._CompletionRequirementContractRow(requirement_id=str(i),requirement=path.name,
        requirement_kind='deliverable',evidence_subject_id=path.name,eligible_evidence_tool_names=('generate',))
        for i,path in enumerate(paths)]
    def decision(rows,ids):
        return agent._CompletionReviewDecision('complete',(),all_material_requirements_enumerated=True,
            material_requirements=tuple(agent._CompletionReviewMaterialRequirement(requirement_id=row.requirement_id,
                requirement=row.requirement,requirement_kind=row.requirement_kind,satisfied=True,draft_covered=True,
                evidence_invocation_ids=(identity,)) for row,identity in zip(rows,ids,strict=True)))
    ids=[record.invocation_id for record in records]
    assert agent._forced_completion_review_is_grounded(decision(contract,ids),tool_results=[result,*records],
        requirement_contract=contract,source_record_fingerprints=fingerprints)
    assert not agent._forced_completion_review_is_grounded(decision(contract,[ids[0],ids[0]]),tool_results=[result,*records],
        requirement_contract=contract,source_record_fingerprints=fingerprints)
    fact=[agent.replace(contract[0],requirement_kind='fact')]
    assert not agent._forced_completion_review_is_grounded(decision(fact,[ids[0]]),tool_results=[result,*records],
        requirement_contract=fact,source_record_fingerprints=fingerprints)


def test_distinct_native_deliverables_may_share_a_supporting_source_without_sharing_file_identity(tmp_path):
    import nullion.agent_orchestrator as agent
    paths,registry,producer=setup_outputs(tmp_path)
    records,fingerprints=artifact_review_records(paths,[producer],registry=registry,allowed_roots=[tmp_path])
    shared=ToolResult('shared-input','read','completed',{'record':{'id':'画像','width':37}})
    rows=[agent._CompletionRequirementContractRow(str(i),path.name,'deliverable',('generate','read'),path.name)
        for i,path in enumerate(paths)]
    def grounded(primary):
        decision=agent._CompletionReviewDecision('complete',(),all_material_requirements_enumerated=True,
            material_requirements=tuple(agent._CompletionReviewMaterialRequirement(row.requirement_id,row.requirement,
                row.requirement_kind,True,True,tuple([*([identity] if identity else []),shared.invocation_id]))
                for row,identity in zip(rows,primary,strict=True)))
        return agent._forced_completion_review_is_grounded(decision,tool_results=[producer,*records,shared],
            requirement_contract=rows,source_record_fingerprints=fingerprints)
    assert grounded([record.invocation_id for record in records])
    assert not grounded([records[0].invocation_id,records[0].invocation_id])
    assert not grounded([None,None])


def test_package_constraints_reuse_only_their_declared_verified_file_members(tmp_path):
    import nullion.agent_orchestrator as agent
    paths,registry,result=setup_outputs(tmp_path)
    records,fingerprints=artifact_review_records(paths,[result],registry=registry,allowed_roots=[tmp_path])
    # The final files may have been repaired by a permitted producer other
    # than the original tool anticipated by the requirements planner.
    rows=[agent._CompletionRequirementContractRow(str(i),path.name,'deliverable',('native_create',),path.name)
          for i,path in enumerate(paths)]
    rows.append(agent._CompletionRequirementContractRow('package','Exactly two final files','constraint',(),
        'package',tuple(path.name for path in paths)))
    coverage=[agent._CompletionReviewMaterialRequirement(row.requirement_id,row.requirement,row.requirement_kind,
        True,True,(record.invocation_id,)) for row,record in zip(rows[:2],records,strict=True)]
    coverage.append(agent._CompletionReviewMaterialRequirement('package',rows[-1].requirement,'constraint',True,True,
        tuple(record.invocation_id for record in records)))
    def grounded(contract=rows,requirements=coverage):
        return agent._forced_completion_review_is_grounded(agent._CompletionReviewDecision('complete',(),
            all_material_requirements_enumerated=True,material_requirements=tuple(requirements)),
            tool_results=[result,*records],requirement_contract=contract,source_record_fingerprints=fingerprints)
    assert grounded()
    assert not grounded(requirements=[*coverage[:2],agent.replace(coverage[-1],evidence_invocation_ids=(records[0].invocation_id,))])
    assert not grounded(contract=[*rows[:2],agent.replace(rows[-1],artifact_member_subject_ids=('missing',))])
    assert not grounded(contract=[*rows[:2],agent.replace(rows[-1],artifact_member_subject_ids=(paths[0].name,'package'))])
    assert not grounded(contract=[agent.replace(rows[0],requirement_kind='fact'),*rows[1:]],
        requirements=[agent.replace(coverage[0],requirement_kind='fact'),*coverage[1:]])


def test_package_contract_rejects_unknown_fact_and_nested_members():
    import json
    from nullion.agent_orchestrator import _parse_completion_requirement_contract
    rows=[{'requirement':'ファイル','requirement_kind':'deliverable','evidence_subject_id':'file',
           'eligible_evidence_tool_names':['generate']},
          {'requirement':'Paquete exacto','requirement_kind':'constraint','evidence_subject_id':'package',
           'artifact_member_subject_ids':['file'],'eligible_evidence_tool_names':[]}]
    def parse(values):
        return _parse_completion_requirement_contract({'content':[{'type':'text','text':json.dumps({
            'all_material_requirements_enumerated':True,'material_requirements':values})}]},registered_tool_names={'generate'})
    assert len(parse(rows))==2
    assert not parse([{**rows[0],'requirement_kind':'fact'},rows[1]])
    assert not parse([rows[0],{**rows[1],'artifact_member_subject_ids':['missing']}])
    assert not parse([rows[0],{**rows[1],'artifact_member_subject_ids':['package']}])

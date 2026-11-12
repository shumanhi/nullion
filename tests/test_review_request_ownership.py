import json
from types import SimpleNamespace
import pytest
from nullion.tools import ToolResult,ToolRegistry
from nullion import agent_orchestrator as a
from nullion.source_observation_contract import source_capture_review_records


def test_helper_catalog_and_prepared_versions_do_not_replan_request_but_context_changes_do():
    calls=[]
    class Model:
        def create(self,**kw):
            payload=json.loads(kw['messages'][0]['content'][0]['text']);calls.append(payload)
            assert 'artifact_delivery_contract' not in payload
            return {'content':[{'type':'text','text':json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.csv'],'material_requirements':[{'requirement':'Deliver requested data','requirement_kind':'deliverable','evidence_subject_id':'data','eligible_evidence_tool_names':['file_write']}]})}]}
    state={'orchestrator':SimpleNamespace(model_client=Model()),'tool_registry':ToolRegistry(),'user_message':'保存したデータをCSVでください。','tool_flow_context':{'artifact_extensions':['.csv','.jpg']},'selected_artifact_paths':['/tmp/render.jpg']}
    result=ToolResult('first','file_write','completed',{'path':'/tmp/data.csv'})
    first=a._extract_completion_requirement_contract(state,tool_results=[result]);assert len(calls)==1
    state['selected_artifact_paths']=['/tmp/repaired.csv'];state['tool_flow_context']['artifact_extensions']=['.zip','.jpg']
    newer=[result,ToolResult('inspect','file_read','completed',{'path':'/tmp/repaired.csv','content':'37'}),ToolResult('source','new_reader','completed',{'value':37})]
    assert a._extract_completion_requirement_contract(state,tool_results=newer)==first and len(calls)==1
    assert a._required_attachment_extensions_from_turn_state(state)==('.csv',)
    state['user_message']='新しいデータをCSVでください。'
    assert a._extract_completion_requirement_contract(state,tool_results=newer) and len(calls)==2


@pytest.mark.parametrize('kind',['fact','constraint','comparison_side','action','deliverable'])
def test_only_bound_factual_source_records_can_supply_new_adapter_proof(kind):
    source=ToolResult('native','new_reader','completed',{'record':{'key':'記録','value':37},'source_record_collections':[{'path':['record'],'identity_fields':['key'],'cardinality':'one'}]})
    records,fingerprints=source_capture_review_records([source]);assert len(records)==1
    row=a._CompletionRequirementContractRow('r1','Verify outcome',kind,('old_reader',),'one')
    review=a._CompletionReviewDecision('complete',all_material_requirements_enumerated=True,material_requirements=(a._CompletionReviewMaterialRequirement('r1','Verify outcome',kind,True,True,(records[0].invocation_id,)),))
    valid=a._forced_completion_review_is_grounded(review,tool_results=records,requirement_contract=(row,),source_record_fingerprints=fingerprints)
    assert valid is (kind in {'fact','constraint','comparison_side'})
    assert not a._forced_completion_review_is_grounded(review,tool_results=records,requirement_contract=(row,),source_record_fingerprints={})


@pytest.mark.parametrize('cardinality',['one','many','invalid'])
def test_singleton_source_records_need_explicit_object_cardinality(cardinality):
    data={'record':{'title':'展示','url':'https://example.org','details':'Admission 37'},'source_record_collections':[{'path':['record'],'identity_fields':['title','url'],'cardinality':cardinality}]}
    original=json.dumps(data,sort_keys=True)
    records,fingerprints=source_capture_review_records([ToolResult('native','detail','completed',data)])
    assert bool(records)==(cardinality=='one') and bool(fingerprints)==bool(records)
    assert json.dumps(data,sort_keys=True)==original


def test_reviewed_manifest_formats_survive_final_recording_but_not_new_versions(tmp_path):
    from nullion.artifact_lifecycle import record_delivery_manifest,reviewed_delivery_extensions
    path=str(tmp_path/'report.html');rows=[ToolResult('write','file_write','completed',{'artifact_path':path})]
    record_delivery_manifest(rows,[path],review_status='completed',required_extensions=['.html'])
    assert reviewed_delivery_extensions(iter(rows))==('.html',)
    record_delivery_manifest(rows,[path])
    assert reviewed_delivery_extensions(rows)==('.html',)
    rows.append(ToolResult('rewrite','file_write','completed',{'artifact_path':path}))
    assert reviewed_delivery_extensions(rows) is None
    record_delivery_manifest(rows,[],review_status='blocked')
    assert reviewed_delivery_extensions(rows) is None


def test_reviewed_parallel_manifest_formats_merge_but_parent_selection_supersedes_them(tmp_path):
    from nullion.artifact_lifecycle import record_delivery_manifest,reviewed_delivery_extensions
    a,b=str(tmp_path/'a.html'),str(tmp_path/'b.csv')
    left=[ToolResult('left','file_write','completed',{'artifact_path':a})];right=[ToolResult('right','file_write','completed',{'artifact_path':b})]
    record_delivery_manifest(left,[a],review_status='completed',required_extensions=['.html'])
    record_delivery_manifest(right,[b],review_status='completed',required_extensions=['.csv'])
    assert set(reviewed_delivery_extensions(left+right))=={'.html','.csv'}
    record_delivery_manifest(left+right,[a],review_status='completed',required_extensions=['.html'])
    assert reviewed_delivery_extensions(left+right)==('.html',)


@pytest.mark.parametrize('change',[{}, {'completion_review_owner':'producer'}, {'required_artifact_extensions':['.pdf']}, {'scheduled_task_run':False}])
def test_old_scheduler_owner_hint_cannot_bypass_common_outcome_review(change):
    context={'scheduled_task_run':True,'requires_artifact_delivery':True,'completion_review_owner':'cron_delivery','required_artifact_extensions':['.html'],**change}
    assert a._completion_review_required({'tool_flow_context': context},
        tool_results=[ToolResult('source', 'sensor_read', 'completed', {'value': 7})])

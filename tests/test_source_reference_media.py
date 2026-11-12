"""Only current selected source media reach internal decisions."""
import pytest
from nullion.agent_orchestrator import _source_reference_media_blocks,_source_reference_media_policy


def test_unrelated_history_media_are_never_added_to_internal_review():
    old={'type':'image','source':{'type':'base64','media_type':'image/png','data':'old'}}
    state={'messages':[{'role':'user','content':[old]}]}
    assert _source_reference_media_blocks(state)==[] and _source_reference_media_policy(state)==''


def test_selection_uses_selected_media_without_turning_inputs_into_deliverables():
    from nullion.agent_orchestrator import _artifact_delivery_selection_request
    selected={'type':'image','source':{'type':'base64','media_type':'image/png','data':'selected'}}
    state={'source_reference_media_blocks':[selected,{'type':'tool_use','name':'unsafe'}],
        'artifact_selection_candidates':[],'user_message':'選択してください','tool_results':[]}
    request=_artifact_delivery_selection_request(state)
    media=[b for b in request['messages'][0]['content'] if b.get('type')=='image']
    assert media==[selected] and len(request['tools'])==1
    assert request['tools'][0]['name']=='select_artifact_delivery'
    assert 'do not independently establish current availability' in request['system']


@pytest.mark.parametrize('caption',['Use this source','Usa esta fuente','この資料を使ってください'])
def test_resume_recovers_selected_request_media_without_older_caption_leak(caption):
    from nullion.agent_orchestrator import _source_media_from_request_snapshot
    image={'type':'image','source':{'type':'base64','data':'selected','media_type':'image/png'}}
    old={'role':'user','content':[{'type':'text','text':caption},image]}
    snapshot=[old,{'role':'assistant','content':[{'type':'tool_use','name':'file_write'}]}]
    assert _source_media_from_request_snapshot(snapshot,caption)==[image]
    snapshot.insert(1,{'role':'user','content':[{'type':'text','text':caption}]})
    assert _source_media_from_request_snapshot(snapshot,caption)==[]
    assert _source_media_from_request_snapshot(snapshot,'different request')==[]


def test_resume_retains_completed_artifact_and_exact_pending_receipt(tmp_path,monkeypatch):
    from nullion import agent_orchestrator as module
    from nullion.tools import ToolRegistry
    artifact=tmp_path/'report.csv';artifact.write_text('name,value\ncedar,1\n')
    captured={}
    class Graph:
        def invoke(self,state,config):
            captured.update(state)
            return {'result':module.TurnResult(turn_id='resume',final_text='Saved',artifacts=state['artifacts']),
                'cleanup_done':True}
    monkeypatch.setattr(module,'_compiled_agent_turn_graph',lambda:Graph())
    batch=[{'type':'tool_use','id':'saved-1','name':'spreadsheet_create','input':{'name':'report'}},
        {'type':'tool_use','id':'read-2','name':'source_fixture','input':{}}]
    pending=[{'invocation_id':'receipt-1','tool_name':'spreadsheet_create','status':'completed',
        'output':{'artifact_path':str(artifact)},'error':None,'tool_use_id':'saved-1','arguments':{'name':'report'}}]
    result=module.AgentOrchestrator(model_client=object()).resume_turn(conversation_id='web:fixture',principal_id='web:fixture',
        user_message='資料を使ってください',messages_snapshot=[{'role':'user','content':[{'type':'text','text':'資料を使ってください'}]},
        {'role':'assistant','content':batch}],tool_registry=ToolRegistry(),policy_store=None,approval_store=None,
        suspended_tool_calls=pending)
    assert result.artifacts==[str(artifact)]
    receipt=captured['pending_tool_receipts']['saved-1']
    assert receipt['arguments']=={'name':'report'} and receipt['result'].invocation_id=='receipt-1'
    assert len(captured['tool_results'])==1 and captured['initial_tool_content']==batch

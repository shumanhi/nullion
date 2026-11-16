from copy import deepcopy
import pytest
from scripts.live_stage_chat_contract_probe import _assert_fresh_scheduler_identity


@pytest.mark.parametrize('visible',['記録 37','対象 ID37'])
@pytest.mark.parametrize('mutation',['none','wrong_id','wrong_name','disabled','ran','effect','no_read','unidentified'])
def test_fresh_scheduler_proof_binds_visible_identity_and_native_state(visible,mutation):
    row={'id':'ID37','name':'記録 37','enabled':True,'has_last_result':False}
    turn={'tool_results':[{'tool_name':'list_crons','status':'completed','output':{'crons':[row]}}]}
    if mutation=='wrong_id':row['id']='other'
    if mutation=='wrong_name':row['name']='別の記録'
    if mutation=='disabled':row['enabled']=False
    if mutation=='ran':row['has_last_result']=True
    if mutation=='effect':turn['tool_results'].append({'tool_name':'run_cron','status':'completed'})
    if mutation=='no_read':turn['tool_results'][0]['status']='failed'
    if mutation=='unidentified':visible='別の記録'
    before=deepcopy(turn)
    if mutation=='none':
        assert _assert_fresh_scheduler_identity(turn,cron_id='ID37',cron_name='記録 37',visible_text=visible)['enabled']
    else:
        with pytest.raises(AssertionError):
            _assert_fresh_scheduler_identity(turn,cron_id='ID37',cron_name='記録 37',visible_text=visible)
    assert turn==before

"""Tool use alone cannot make a partial live status answer pass acceptance."""
import json,sqlite3
import pytest
from scripts import live_stage_chat_contract_probe as probe

@pytest.mark.parametrize('fulfilled',[True,False])
def test_fresh_check_probe_requires_its_own_completed_turn(tmp_path,monkeypatch,fulfilled):
    db=tmp_path/'runtime.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE conversation_events (payload TEXT,updated_at TEXT)')
    jobs={};cid=[]
    monkeypatch.setattr(probe,'_get_json',lambda *args,**kwargs:list(jobs.values()))
    monkeypatch.setattr(probe,'_delete_json',lambda url,**kw:jobs.pop(url.rsplit('/',1)[-1]))
    def post(url,payload,timeout):
        if url.endswith('/api/crons'):
            jobs['abc12345']={**payload,'id':'abc12345'}
            return {'ok':True,'job':jobs['abc12345']},1
        if not cid:cid.append(payload['conversation_id']);jobs['abc12345']['enabled']=True
        if payload['text']=='Do a fresh check':
            with sqlite3.connect(db) as conn:
                conn.execute('INSERT INTO conversation_events VALUES (?,?)',(json.dumps({
                    'conversation_id':cid[0],'event_type':'conversation.chat_turn','user_message':payload['text'],
                    'created_at':'2028-01-01T00:00:00+00:00','response_fulfilled':fulfilled,
                    'assistant_reply':'Task is enabled.','tool_results':[{'tool_name':'list_crons','status':'completed'}]}),'2028-01-01'))
        return {'text':'Task is enabled.','artifacts':[]},1
    monkeypatch.setattr(probe,'_post_json',post)
    monkeypatch.setattr(probe,'_event_tool_names_with_poll',lambda *args,**kw:{'toggle_cron'})
    monkeypatch.setattr(probe,'_tool_names_for_user_message_with_poll',lambda *args,**kw:{'list_crons'})
    args=dict(base_url='http://127.0.0.1:8753',runtime_db=db,scenario=probe.SCENARIOS['scheduler_fresh_followup_after_bulk_enable'],
        timeout=2,index=0,runtime_db_available=True)
    if fulfilled:assert probe._run_scheduler_fresh_followup_after_bulk_enable(**args)['followup_tools']==['list_crons']
    else:
        with pytest.raises(AssertionError,match='did not fulfill'):
            probe._run_scheduler_fresh_followup_after_bulk_enable(**args)
    assert not jobs

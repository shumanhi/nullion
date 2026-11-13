"""List-only drafts cannot fulfil a filter amendment; reviewer recovery edits SQLite.

The provider is the only fake in this real chat/tool/worker/delivery workflow.
"""
from datetime import timedelta
import json
import os

import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


def text(value):
    return {'content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}


def tool(name,args):
    return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}


@pytest.mark.parametrize('prompt',['この通知は重要な競争の変化だけに変更して。','Modifica esta alerta para avisar solo de cambios en carreras clave.'])
@pytest.mark.parametrize('review_available',[False])
def test_filter_amendment_is_repaired_then_verified_and_used_by_next_poll(tmp_path,monkeypatch,prompt,review_available):
    from fastapi.testclient import TestClient
    from nullion.live_tracking import LiveTrackerEngine,utcnow
    from nullion.tools import ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    authored='The existing tracker alert condition was updated. Only source-designated competitive races will produce alerts; its saved source, cadence and deadline are unchanged.'
    target={'kind':'semantic','history_mode':'snapshot',
        'instruction':'Report changes only in source-designated competitive races. Ignore source-designated safe races; preserve AP attribution.'}
    state={'safe':10,'competitive':20}; packets=[]
    class Model:
        step=0;repair=False;reviews=0
        def create(self,**kw):
            system=kw.get('system','');names={t['name'] for t in kw.get('tools',())}
            if system.startswith('Evaluate only'):
                p=json.loads(kw['messages'][0]['content'][0]['text']);packets.append(p)
                history=p['snapshot_history'];current=p['observations'][0]
                previous=history.get('previous_observations') or [current]
                changed=current['competitive']!=previous[0]['competitive']
                return text({'status':'matched' if changed else 'no_match','summary':'競争の結果: '+str(current['competitive']),
                    'evidence':[{'step':0,'pointer':'/competitive','kind':'value','value':current['competitive']}] if changed else []})
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'Persist the requested alert filter on the identified tracker',
                        'requirement_kind':'action','evidence_subject_id':'saved-condition','eligible_evidence_tool_names':['update_live_tracker']}]})
            if 'semantic completion reviewer' in system:
                assert 'Execution constraints are not additional reporting requirements.' in system
                assert 'Keep every explicitly requested reporting detail mandatory.' in system
                self.reviews+=1;p=json.loads(kw['messages'][0]['content'][0]['text'])
                if not review_available:raise TimeoutError('Reviewer unavailable')
                updates=[r for r in p['tool_evidence'] if r['tool_name']=='update_live_tracker' and r['status']=='completed']
                complete=bool(updates)
                if complete:
                    config=next(r['configuration'] for r in p['accepted_background_execution'] if r['handle']==ident)
                    assert config['matcher']==target and config['interval_seconds']==900
                else:self.repair=True
                return text({'disposition':'complete' if complete else 'retry','all_material_requirements_enumerated':True,
                    'retry_tool_names':[] if complete else ['update_live_tracker'],
                    'unresolved_requirements':[] if complete else ['The list result does not change the saved condition.'],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':complete,'draft_covered':complete,
                        'evidence_invocation_ids':[updates[-1]['invocation_id']] if complete else []} for r in p['material_requirement_contract']]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1,'embedded_media_extensions':[]})
            if 'request_tool_scope' in names and 'list_live_trackers' not in names:
                return tool('request_tool_scope',{'capabilities':['live_tracking'],'tool_names':['list_live_trackers','update_live_tracker'],'tracking_mode':'live'})
            if self.step==0:
                if not review_available:
                    self.step=2;return tool('update_live_tracker',{'id':ident,'matcher':target})
                self.step=1;return tool('list_live_trackers',{})
            if self.step==1:
                self.step=2;return tool('update_live_tracker',{'id':ident,'matcher':target})
            return text(authored if self.step==2 else 'The tracker is active; checks run every 15 minutes.')
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    registry=app.state.live_tracker_engine.registry
    def read(inv):return ToolResult(inv.invocation_id,inv.tool_name,'completed',dict(state))
    registry.register(ToolSpec('race_read','Read typed race states',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    with TestClient(app) as client:
        app.state.live_tracker_engine.stop();store=app.state.live_tracker_store
        response=client.post('/api/live-trackers',json={'name':'Owned condition test','conversation_id':'web:condition',
            'duration_seconds':3600,'interval_seconds':900,'probe':[{'tool_name':'race_read','arguments':{}}],
            'matcher':{**target,'instruction':'Report all changes.'}})
        assert response.status_code==200,response.text
        before=response.json();ident=before['id']
        engine=LiveTrackerEngine(store,registry=registry,runtime_store=runtime.store,model_client=model,
            deliver=app.state.live_tracker_engine.deliver)
        current,token=store.claim(1);engine.check_once(current,token)
        baseline=store.get(ident)['comparison_snapshot']
        reply=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:condition'})
        assert reply.status_code==200,reply.text
        assert reply.json()['text'].count(authored)==1,reply.json()
        saved=store.get(ident)
        assert saved['matcher']==target and saved['condition_revision']==1,reply.json()
        assert saved['expires_at']==before['expires_at'] and saved['interval_seconds']==900
        assert saved['comparison_snapshot']==baseline and len(store.list('workspace_admin'))==1
        assert model.reviews == 0 and model.step == 2
        reviews=runtime.store.list_recent_conversation_events('web:condition',event_type='conversation.completion_review')
        if review_available:
            assert max(reviews,key=lambda r:r['created_at'])['disposition']=='complete' and any(r['disposition']=='retry' for r in reviews), {'reviews':reviews,'reply':reply.json()}
        for values in [dict(safe=11,competitive=20),dict(safe=12,competitive=21)]:
            state.update(values);current,token=store.claim(1,now=utcnow()+timedelta(minutes=16));engine.check_once(current,token)
        notice=store.claim_notice();assert notice and '21' in notice['text']
        engine._send(notice)
        assert store.claim_notice() is None
        history=client.get('/api/chat/history/web:condition').json()['messages']
        assert len([r for r in history if (r.get('metadata') or {}).get('source')=='live_tracker'])==1
        assert packets[-1]['condition']==target['instruction']
        # HTTP amendment shares exactly the same validation and persistence.
        invalid=client.patch('/api/live-trackers/'+ident,json={'matcher':{'kind':'semantic','instruction':''}})
        assert invalid.status_code==400 and store.get(ident)['matcher']==target
        assert client.patch('/api/live-trackers/'+ident,json={'conversation_id':'web:foreign','matcher':target}).status_code==403
        store.stop(ident,'workspace_admin')

"""The real chat routes synthesize captured observations; only external boundaries are scripted."""
import copy
import json
import os
import time
from pathlib import Path
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')
REPORT=json.loads((Path(__file__).resolve().parents[1]/'fixtures/clinique_source_observations.json').read_text())


def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
def tool(name,value):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':value}]}


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('prompt',[
    'What are some of the top selling clinique facial products, especially for south asian hyperpigmentation',
    '南アジアの肌の色素沈着に特に関連するクリニークの人気の顔用製品は？',
    '¿Qué productos faciales populares de Clinique son relevantes para la hiperpigmentación en piel del sur de Asia?',
])
def test_original_relevance_failure_through_real_chat_routes(tmp_path,monkeypatch,prompt,surface,source_summary_model_reply):
    run_case(tmp_path,monkeypatch,prompt,surface,review=source_summary_model_reply)


@pytest.mark.parametrize('bad_mode',['invented_excerpt','extra_tool','malformed'])
def test_invalid_synthesis_retries_once_without_dumping_or_more_source_calls(tmp_path,monkeypatch,bad_mode,source_summary_model_reply):
    run_case(tmp_path,monkeypatch,'Compare the relevant options.', 'web',bad_mode=bad_mode,review=source_summary_model_reply)


def test_exhausted_synthesis_has_bounded_honest_fallback(tmp_path,monkeypatch):
    run_case(tmp_path,monkeypatch,'Compare the relevant options.','web',bad_mode='always_invalid')


def run_case(tmp_path,monkeypatch,prompt,surface,bad_mode=None,review=None):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_operator,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.plugins.browser_plugin import register_browser_tools,browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.telegram_formatting import format_telegram_text
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_KEY_STORAGE','file');monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available',lambda:True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python',lambda:'/external/python')
    source_calls=[]
    def external_browser(*args,**kwargs):source_calls.append(1);return copy.deepcopy(REPORT)
    monkeypatch.setattr(browser_use_worker,'run_isolated_browser_task',external_browser)
    class Provider:
        calls=0;presentations=0
        def create(self,**kw):
            names={d['name'] for d in kw.get('tools',[])}
            system=str(kw.get('system') or '')
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if 'relationship' in system:return text(json.dumps({'relationship':'independent','effect':'none','confidence':1,'target_index':None}))
            if review is not None:
                reviewed=review(kw)
                if reviewed is not None: return reviewed
            if not names and system:return text('{}')
            self.calls+=1
            if 'present_source_answer' in names:
                self.presentations+=1
                assert names=={'present_source_answer'} and 'text_delta_callback' not in kw
                message=kw['messages'][0]['content'][0]['text'];assert prompt in message
                raw=message.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                context=json.loads(raw);rows={r['value']:r for r in context['evidence']}
                if bad_mode=='always_invalid' or self.presentations==1 and bad_mode=='malformed':return text('Malformed presentation')
                if self.presentations==1 and bad_mode=='extra_tool':return tool('browser_run_task',{'task':'unnecessary retry'})
                chosen=[REPORT['observations'][i] for i in (6,11,12)]
                answer={'intro':'For the concern you mentioned, prioritize these options:',
                    'points':[{'evidence_ids':[rows[r['value']]['id']], 'detail_ids':[next(excerpt['id'] for excerpt in rows[r['value']]['excerpts'] if excerpt['text']==r['quote'].split('\n')[-1])],
                               'relevance':'Relevant to the requested skin concern'} for r in chosen],
                    'limitation_ids':['u0','u1']}
                if self.presentations==1 and bad_mode=='invented_excerpt':answer['points'][0]['detail_ids']=['invented-support']
                return tool('present_source_answer',answer)
            if self.calls==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']})
            if self.calls==2:return tool('browser_run_task',{'task':prompt,'url':'https://www.clinique.com/'})
            raise AssertionError('Ordinary summary/review must not repeat after the source presentation')
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    runtime.active_tool_registry=registry;orchestrator=AgentOrchestrator(model_client=model)
    started=time.perf_counter()
    if surface=='web':
        with TestClient(web_app.create_app(runtime,orchestrator=orchestrator,registry=registry)) as client:
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:relevance','turn_id':'relevance-test'})
            assert result.status_code==200,result.text
            reply=result.json()['text'];conversation='web:relevance'
    else:
        settings=load_settings(env={'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'777','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
        reply=chat_operator._render_chat_turn(runtime,message='/chat '+prompt,chat_id='777',settings=settings,
            model_client=model,agent_orchestrator=orchestrator,append_activity_trace=False)
        conversation='telegram:777'
    assert time.perf_counter()-started<8
    assert len(source_calls)==1 and model.presentations==(2 if bad_mode else 1)
    if bad_mode=='always_invalid':
        assert len(reply)<150 and 'Moisture Surge' not in reply
    else:
        assert reply.replace('™','').count('**Even Better Clinical Dark Spot Clearing Serum**')==1
        assert 'Smart Clinical Repair' not in reply and 'All About Eyes' not in reply
        assert 'additional findings' not in reply and REPORT['unknowns'][2] not in reply
        assert REPORT['unknowns'][0] in reply and REPORT['unknowns'][1] in reply
        assert 'Clinically proven for South Asian skin' not in reply
        rendered,options=format_telegram_text(reply)
        assert '<b>Even Better Clinical' in rendered and '<a href=' in rendered
        assert options['parse_mode']=='HTML'
    events=[e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type')=='conversation.chat_turn']
    assert events and 'present_source_answer' not in {r['tool_name'] for r in events[-1]['tool_results']}
    observed=next(r['output'] for r in events[-1]['tool_results'] if r['tool_name']=='browser_run_task')
    assert len(observed['observations'])>15 # Full evidence stays durable without becoming the reply.


def test_plain_turns_never_enter_slow_source_synthesis(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion import source_observation_contract as contract,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    calls=[]
    def slow_context(*args):
        calls.append(1);time.sleep(.6);raise AssertionError('Plain chat entered source synthesis')
    monkeypatch.setattr(contract,'source_answer_context',slow_context)
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_KEY_STORAGE','file')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    class Provider:
        main_calls=0
        def create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if 'relationship' in system:return text(json.dumps({'relationship':'independent','effect':'none','confidence':1,'target_index':None}))
            if not names and system:return text('{}')
            self.main_calls+=1;assert 'present_source_answer' not in names
            return text('4')
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');samples=[]
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=ToolRegistry())) as client:
        for index,prompt in enumerate(['Return 2+2','数字だけ：2+2','Solo 2+2']*2):
            started=time.perf_counter();r=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:plain-{index}'})
            samples.append(time.perf_counter()-started);assert r.status_code==200 and r.json()['text']=='4',r.text
    average=sum(samples)/len(samples);record_property('plain_chat_average_seconds',average)
    assert average<1.5 and max(samples)<3 and model.main_calls==6 and not calls

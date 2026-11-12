"""Native chat routing keeps costly optional workflows off the ordinary path."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import time
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Native API/SQLite boundary')


def test_optional_worker_requires_exact_requirement_without_slowing_plain_turns(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store, tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv('HOME', str(tmp_path)); monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env')); monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off'); chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db'); registry=tools.ToolRegistry(); heavy=[]
    def read(inv):return tools.ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':37})
    def slow(inv):
        heavy.append(inv.invocation_id);time.sleep(.6)
        return read(inv)
    registry.register(tools.ToolSpec('q0','Lightweight public reader',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.READ,False,5),read)
    registry.register(tools.ToolSpec('h9','Costly delegated workflow',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.READ,False,5,capability_tags=('explicit_scope_only','required_scope_only')),slow)
    class Provider:
        phase=0; mode='plain';calls=0;reflections=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name+str(self.phase),'name':name,'input':args}]}
            if not kw.get('tools'):
                self.reflections+=1
                return text('{"should_propose":false}')
            self.calls+=1;self.phase+=1
            if self.mode=='plain':return text('42')
            if self.phase==1:return call('request_tool_scope',{'capabilities':[],'tool_names':['q0','h9'],'required_tool_names':['q0']})
            if self.phase==2:
                names={row['name'] for row in kw.get('tools',[])}
                return call('h9' if 'h9' in names else 'q0',{})
            assert self.phase==3,kw
            return text('37')
    provider=Provider();runtime.model_client=provider;samples={'plain':[],'read':[]}
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)) as client:
        for index,(plain,read_prompt) in enumerate([('Solo el número 42.','Obtén el valor actual.'),('数字42だけ。','現在の値を取得してください。'),('Return only 42.','Get the current value.')]*2):
            for mode,prompt in [('plain',plain),('read',read_prompt)]:
                provider.mode=mode;provider.phase=0;before=provider.calls;reflections=provider.reflections;cid='web:optional-latency:'+mode+str(index);started=time.monotonic()
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});elapsed=time.monotonic()-started;samples[mode].append(elapsed)
                assert response.status_code==200 and response.json()['text'].strip()==('42' if mode=='plain' else '37'),response.json()
                assert provider.calls-before==(1 if mode=='plain' else 3)
                if mode=='plain':assert provider.reflections==reflections
                turn=next(e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn')
                assert [r['tool_name'] for r in turn['tool_results']]==([] if mode=='plain' else ['request_tool_scope','q0'])
    assert not heavy,heavy
    for rows in samples.values():assert sum(rows)/len(rows)<1 and max(rows)<2, samples

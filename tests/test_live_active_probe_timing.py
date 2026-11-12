import asyncio,json,sys
from types import SimpleNamespace
from scripts import live_stage_chat_contract_probe as probe


def test_three_request_workflow_keeps_single_request_latency_separate(monkeypatch):
    clock=[0.0];wire=[]
    class Socket:
        async def send(self,raw):wire.append(json.loads(raw))
        async def recv(self):
            if clock[0]<160:clock[0]=160;ident='turn-live-artifact'
            else:clock[0]=200;ident='turn-live-sure'
            return json.dumps({'type':'done','turn_id':ident})
    class Connection:
        async def __aenter__(self):return Socket()
        async def __aexit__(self,*a):pass
    monkeypatch.setitem(sys.modules,'websockets',SimpleNamespace(connect=lambda *a,**kw:Connection()))
    monkeypatch.setattr(probe.time,'perf_counter',lambda:clock[0])
    async def weather(*a,**kw):clock[0]=80
    async def pause(seconds):clock[0]+=seconds
    monkeypatch.setattr(probe,'_send_ws_turn_and_wait_done',weather)
    monkeypatch.setattr(probe.asyncio,'sleep',pause)
    monkeypatch.setattr(probe,'_turn_payload_with_poll',lambda *a:{'parent_turn_id':'turn-live-weather'})
    monkeypatch.setattr(probe,'_chat_turn_for_user_message_with_poll',lambda *a:{'assistant_reply':'Confirmed'})
    monkeypatch.setattr(probe,'_event_tool_names',lambda *a:[])
    checked=[];monkeypatch.setattr(probe,'_assert_contract',lambda *a,**kw:checked.append(kw['elapsed_ms']))
    value=asyncio.run(probe._run_active_followup_context_guard_async(base_url='http://127.0.0.1:8753',runtime_db=None,
        scenario=probe.SCENARIOS['active_followup_context_guard'],timeout=240,index=0,runtime_db_available=False))
    assert value['workflow_total_ms']==200000
    assert value['request_elapsed_ms']=={'turn-live-weather':80000,'turn-live-artifact':80000,'turn-live-sure':119750}
    assert checked==[119750] and value['elapsed_ms']==119750
    assert len(wire)==2

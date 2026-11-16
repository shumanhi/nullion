"""Real stage WebSocket final delivery, independently of HTTP fallback."""
import asyncio
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in live stage')


@pytest.mark.parametrize('stream',[False,True])
def test_live_socket_returns_a_matching_terminal_result(stream,record_property):
    import websockets
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753')
    assert base=='http://127.0.0.1:8753'
    cid='web:live-ui-audit:socket-'+uuid4().hex
    turn='turn-socket-'+uuid4().hex
    events=[];started=time.monotonic()
    async def request():
        async with websockets.connect(base.replace('http:','ws:')+'/ws/chat',origin=base) as ws:
            await ws.send(json.dumps({'text':'Reply with only SOCKET_CHECK37.','conversation_id':cid,
                'turn_id':turn,'stream':stream}))
            deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                event=json.loads(await asyncio.wait_for(ws.recv(),max(.1,deadline-time.monotonic())))
                events.append(event)
                if event.get('turn_id')==turn and event.get('type')=='done':return
            raise AssertionError('Missing matching WebSocket terminal result')
    try:asyncio.run(request())
    finally:
        root=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))/'socket-delivery'
        root.mkdir(parents=True,exist_ok=True)
        (root/(turn+'.json')).write_text(json.dumps({'conversation_id':cid,'turn_id':turn,
            'elapsed_seconds':time.monotonic()-started,'events':events},ensure_ascii=False,indent=2))
    assert not any(e.get('type')=='error' for e in events),events
    assert 'SOCKET_CHECK37' in ''.join(str(e.get('text','')) for e in events)
    assert sum(e.get('type')=='done' and e.get('turn_id')==turn for e in events)==1
    record_property('socket_elapsed_seconds',time.monotonic()-started)

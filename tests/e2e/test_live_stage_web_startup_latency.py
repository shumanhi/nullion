"""Opt-in real-provider startup timing through the actual stage Web composer."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web,catalogue
from tests.e2e.test_live_stage_code_mode import _submit

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Explicit live stage acceptance')


def test_live_stage_plain_requests_preserve_fast_path_and_report_startup_timings(live_web,record_property):
    proofs=[]
    for prompt,marker in [('Reply with only ALPHA37.','ALPHA37'),
            ('Responde solo BRAVO37.','BRAVO37'),('CHARLIE37だけ返してください。','CHARLIE37')]:
        response,turn,elapsed=_submit(live_web,prompt,timeout=60)
        assert marker in response['text'],response
        assert not turn.get('tool_results'),turn.get('tool_results')
        rows=catalogue._conversation_event_payloads(live_web[3],live_web[4],event_type='conversation.latency_timing')
        timing=next(row for row in reversed(rows) if row.get('turn_id')==turn['turn_id'] and row.get('surface')=='messaging')
        phases=timing.get('phase_timings',())
        memory=next(row['ms'] for row in phases if row.get('phase')=='memory_context')
        proofs.append({'elapsed_ms':elapsed,'memory_ms':memory,'response':response,'turn_id':turn['turn_id'],'timing':timing})
    target=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR']);target.mkdir(parents=True,exist_ok=True)
    (target/'startup-latency.json').write_text(json.dumps(proofs,ensure_ascii=False,indent=2))
    live_web[0].screenshot(path=str(target/'startup-latency.png'),full_page=True)
    average=sum(row['elapsed_ms'] for row in proofs)/len(proofs)
    assert average<15000 and max(row['elapsed_ms'] for row in proofs)<30000,proofs
    record_property('mean_plain_turn_seconds',average/1000)
    record_property('mean_memory_context_ms',sum(row['memory_ms'] for row in proofs)/len(proofs))

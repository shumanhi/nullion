"""Saved work must be verified even when the producer initially does nothing."""
import json
import os
from datetime import UTC, datetime

import pytest

from test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')




@pytest.mark.parametrize('task', ['短い挨拶を一つ。', 'Envía un saludo breve.'])
def test_scheduled_reasoned_answer_can_complete_without_invoking_tools(tmp_path, monkeypatch, task):
    from nullion import crons
    from nullion.tools import ToolRegistry
    runtime = setup_runtime(tmp_path, monkeypatch)
    class Model:
        producer = 0
        reviews = 0
        def create(self, **kw):
            if str(kw.get('system','')).startswith('You are a semantic completion reviewer'):self.reviews += 1
            protocol = workflow_provider_reply(kw, completion=True, reasoned=True)
            if protocol is not None:return protocol
            self.producer += 1
            return text_response('こんにちは。')
    job = crons.add_cron('挨拶','* * * * *',task,enabled=False,delivery_channel='telegram',delivery_target='2222')
    model=Model(); deliveries,send=telegram_boundary(monkeypatch)
    result=run_scheduled(job,model,runtime,ToolRegistry(),send)
    assert result['response_fulfilled'] is True and result['execution_outcome']['execution_succeeded'] is True
    assert not result['tool_results'] and model.producer == 1 and (model.reviews == 0)
    assert len(deliveries)==1 and 'こんにちは。' in deliveries[0]['text']

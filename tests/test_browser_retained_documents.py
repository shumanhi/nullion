"""Rejected final claims cannot erase the browser's actual earlier reads."""
import asyncio
import json
from types import SimpleNamespace

from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_use_adapter import PageEvidence, BrowserReport
from nullion.source_observation_contract import compact_source_observations
from nullion.retained_work_context import inspect_retained
from nullion.tools import ToolResult


def test_prior_page_text_survives_rejected_report_and_current_page_change():
    page={'url':'https://example.com/first','text':'Flight A\n$497 total\nNovember 18–22'}
    async def read():return dict(page)
    evidence=PageEvidence('operator',BrowserPolicy(),text_capture=read)
    def capture():
        state=SimpleNamespace(url=page['url'],title='Flight listings',screenshot=None,
            dom_state=SimpleNamespace(llm_representation=lambda:page['text']))
        asyncio.run(evidence.capture(state))
    capture();page.update(url='https://example.com/second',text='Flight B\n$956 total\nNovember 25–29');capture()
    evidence.retain(BrowserReport(goal_status='answered',next_action='report',observations=[
        {'label':'First fare','value':'$497','source_id':0}]))
    output=evidence.result(error='Step limit reached',elapsed=30,steps=10,done=False)
    assert not output['observations'] and output['report_status']=='blocked'
    assert output['rejected_observations']==1
    assert output['captured_documents'][0]['source_url']=='https://example.com/first'
    assert output['captured_documents'][0]['main_document_text']=='Flight A\n$497 total\nNovember 18–22'
    result=ToolResult('original-browser-read','browser_run_task','completed',output)
    inspected=inspect_retained({'requests':[{'invocation_id':result.invocation_id,
        'json_pointer':'/captured_documents/0/main_document_text','offset':0,'length':12000}]},[result])
    assert '$497' in json.dumps(inspected) and '$956' not in json.dumps(inspected)
    compact=compact_source_observations(output,max_chars=2048)
    assert compact['captured_document_count']==2
    assert compact['captured_document_index'][0]['json_pointer']=='/captured_documents/0'
    assert 'captured_documents' not in compact
    assert not compact['observations'], 'Raw reads never become accepted observations'
    assert '$497' not in json.dumps(compact['captured_document_index'])
    assert len(json.dumps(compact,ensure_ascii=False))<=2048
    assert not output['artifact_paths'] and not output['artifact_descriptors']

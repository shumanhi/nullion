"""Real provider, Web upload, retained source work and generated file contents."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in live stage')


def test_large_uploaded_source_preserves_middle_record_in_delivered_report(live_web,tmp_path):
    page,frames,errors,database,conversation,base=live_web
    records=[{'record_id':f'background-{i}','kind':'background','notes':'Supporting context. '*35} for i in range(120)]
    required={'record_id':'required-aurora','kind':'required','name':'Aurora', 'quantity':73,
              'deadline':'2030-06-10','url':'https://example.com/aurora?record=73&scope=original'}
    records.insert(60,required)
    upload=tmp_path/'source-records.json';upload.write_text(json.dumps({'records':records},ensure_ascii=False))
    page.locator('#file-input').set_input_files(str(upload))
    response,turn,elapsed=_submit(live_web,
        'Create one downloadable HTML report from the attached JSON. Include only the record with kind="required", '
        'preserving its name, quantity, date and exact clickable URL. Exclude the background records. '
        'Give me the report file and a brief confirmation.',timeout=240)
    artifacts=[a for a in response.get('artifacts',[]) if catalogue._artifact_name(a).endswith('.html')]
    assert len(artifacts)==1,(response,elapsed)
    data,_=catalogue._artifact_bytes(base,artifacts[0],timeout=30)
    html=data.decode('utf-8')
    report_page=page.context.new_page()
    try:
        report_page.set_content(html,wait_until='load')
        _assert_report(report_page,required)
    finally:
        report_page.close()
    root=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'));root.mkdir(parents=True,exist_ok=True)
    (root/'delivered-report.html').write_bytes(data)
    (root/'source-compaction-proof.json').write_text(json.dumps({'conversation_id':conversation,
        'turn_id':turn['turn_id'],'elapsed_ms':elapsed,'source_bytes':upload.stat().st_size,
        'required_record':required,'reply':response['text'],'artifact_bytes':len(data)},ensure_ascii=False,indent=2))
    assert not errors


def _assert_report(page,required):
    visible=page.locator('body').inner_text()
    assert 'Aurora' in visible and '73' in visible
    assert required['url'] in page.locator('a[href]').evaluate_all('(links)=>links.map(a=>a.href)')
    assert required['deadline'] in visible and 'background-60' not in visible

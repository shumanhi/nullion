"""Additional live Web cases for final revisions and truthful source answers."""
import io
import json
import os
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import pytest
from tests.e2e.test_live_stage_web_matrix import catalogue,live_web,_submit

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in real stage Web acceptance')


def test_live_web_only_final_document_survives_delivery_and_reload(live_web):
    from docx import Document
    from playwright.sync_api import expect
    page,frames,errors,database,conversation,base=live_web
    token=uuid4().hex[:10];draft=f'working_{token}.docx';final=f'published_{token}.docx'
    response,turn,elapsed=_submit(live_web,
        f'Create a temporary working document named {draft} containing "Inventory working copy: 10 units." '
        f'Then create {final} containing "Inventory approved copy: 12 units." '
        'Deliver only the approved DOCX. Keep the working copy internal, and inspect the approved file before delivery.',timeout=240)
    assert len(response['artifacts'])==1,response
    artifact=response['artifacts'][0];assert Path(artifact['name']).name==final,artifact
    data,_=catalogue._artifact_bytes(base,artifact,timeout=30)
    text='\n'.join(p.text for p in Document(io.BytesIO(data)).paragraphs)
    assert '12 units' in text and '10 units' not in text,text
    assert page.locator('#messages a[download]').filter(has_text=draft).count()==0
    history=catalogue._get_json(base+'/api/chat/history/'+conversation+'?turn_ids='+quote(json.dumps([turn['turn_id']])),timeout=30)
    receipts=history['terminal_replies'];assert len(receipts)==1,history
    assert [Path(row['name']).name for row in receipts[0]['artifacts']]==[final],history
    page.reload(wait_until='domcontentloaded')
    expect(page.locator('#messages a[download]').filter(has_text=final)).to_have_count(1,timeout=15000)
    assert page.locator('#messages a[download]').filter(has_text=draft).count()==0
    assert elapsed<180000 and not errors,(elapsed,errors)


def test_live_web_missing_public_fact_is_explained_once_without_losing_findings(live_web):
    source=os.environ.get('NULLION_LIVE_BOUNDED_COMPARISON_SOURCE')
    if not source:pytest.skip('Controlled source required; example.com no longer publishes a heading')
    response,turn,elapsed=_submit(live_web,
        'Read '+source+'/public-fact/ and tell me its main heading and the exact number of visitors it had yesterday. '
        'If the page does not publish that figure, say so clearly without inventing it. Do not create files.',timeout=240)
    text=response['text']
    assert 'Recorded sample facts' in text and not response['artifacts'],response
    assert 'visitor' in text.lower(),text
    assert text.count('Not verified from this check:')<=1,text
    assert "I couldn't complete this request because" not in text,text
    assert any(row['status']=='completed' and row['tool_name'].startswith('browser_') for row in turn.get('tool_results',[])),turn
    assert elapsed<180000,elapsed


def test_live_web_compact_pdf_summary_keeps_heading_and_bullets_together(live_web):
    from pypdf import PdfReader
    response, turn, elapsed = _submit(live_web,
        'Create one short, polished, single-page PDF report with the title "Project summary", '
        'a table listing Apollo 31, Artemis 22, and Voyager 17, then a "Recommendations" heading '
        'followed immediately by two short bullets: Prioritize Apollo. Review Voyager next week. '
        'Keep the heading and both bullets together. Fit the entire report on one page. '
        'Attach only the final PDF; no research or extra files are needed.', timeout=180)
    assert len(response['artifacts']) == 1 and response['artifacts'][0]['name'].lower().endswith('.pdf'), response
    body, _ = catalogue._artifact_bytes(live_web[-1], response['artifacts'][0], timeout=30)
    reader = PdfReader(io.BytesIO(body)); assert len(reader.pages) == 1
    text = reader.pages[0].extract_text()
    assert all(value in text for value in ('Apollo', '31', 'Artemis', '22', 'Voyager', '17', 'Recommendations',
                                         'Prioritize Apollo', 'Review Voyager next week')), text
    assert sum(row['tool_name'] == 'pdf_create' and row['status'] == 'completed' for row in turn['tool_results']) == 1
    assert elapsed < 120000, elapsed


def test_live_web_dashboard_polling_during_generation_keeps_final_file_and_history(live_web):
    token=uuid4().hex[:10]; filename='status_poll_'+token+'.csv'
    response,turn,elapsed=_submit(live_web,
        'Create one CSV named '+filename+' with columns Name and Value, and exactly these rows: '
        'Current 31 and Approved 17. Read back the created file, then attach only that final CSV.',
        timeout=150,poll_full_status=True)
    assert len(response['artifacts'])==1 and response['artifacts'][0]['name']==filename,response
    body,_=catalogue._artifact_bytes(live_web[-1],response['artifacts'][0],timeout=30)
    assert b'Current,31' in body and b'Approved,17' in body,body
    assert any(row['tool_name']=='file_write' and row['status']=='completed' for row in turn['tool_results'])
    reads=[row for row in turn['tool_results'] if row['tool_name'] in {'file_read','terminal_exec'} and row['status']=='completed']
    assert any('Current,31' in json.dumps(row.get('output')) and 'Approved,17' in json.dumps(row.get('output')) for row in reads),reads
    history=catalogue._get_json(live_web[-1]+'/api/chat/history/'+live_web[4]+'?turn_ids='+quote(json.dumps([turn['turn_id']])),timeout=30)
    assert len(history['terminal_replies'])==1 and len(history['terminal_replies'][0]['artifacts'])==1,history
    assert elapsed<120000,elapsed


def test_live_web_existing_filename_lookup_does_not_recreate_or_attach_input(live_web):
    from uuid import uuid4
    page,frames,errors,database,conversation,base=live_web
    filename='stage_identity_'+uuid4().hex[:10]+'.csv'
    made,created,_=_submit(live_web,'Create '+filename+' with the exact CSV content Name,Value followed by Cedar,37. Attach that CSV only.',timeout=120)
    assert len(made['artifacts'])==1,made
    response,turn,_=_submit(live_web,'Search the workspace for '+filename+' and tell me its exact filename only. Do not attach, rewrite, copy or create any files.',timeout=120)
    assert filename in response['text'] and not response['artifacts'],response
    assert any(r['tool_name']=='file_search' and r['status']=='completed' for r in turn.get('tool_results',[])),turn
    assert not any(r['tool_name'] in {'file_write','file_patch','pdf_create','document_create'} for r in turn.get('tool_results',[])),turn
    assert not errors


@pytest.mark.parametrize('language',['en','es','ja'])
def test_live_web_account_read_waits_for_explicit_source_choice(live_web,language):
    import re
    prompt={
        'en':'Check whether an unspecified external-account event happened. Discover authorized sources if needed, but ask me which source to check before reading any account data.',
        'es':'Comprueba si ocurrió algo en una cuenta externa no especificada. Puedes descubrir las fuentes autorizadas, pero pregúntame qué fuente consultar antes de leer datos de cualquier cuenta.',
        'ja':'特定されていない外部アカウントで何か起きたか確認したいです。必要なら利用可能な情報源を調べてもよいですが、アカウントのデータを読む前に、どの情報源を確認するか私に質問してください。',
    }[language]
    response,turn,elapsed=_submit(live_web,prompt,timeout=120)
    result={'language':language,'conversation_id':live_web[4],'response':response,'turn':turn,'elapsed_ms':elapsed}
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/('account-read-choice-'+language);out.mkdir(parents=True,exist_ok=True)
    (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    names={row['tool_name'] for row in turn['tool_results']}
    assert 'request_tool_scope' in names,names
    assert not names.intersection({'email_search','email_read','email_attachment_read','calendar_list','connector_request'}),turn
    scopes=[r['output'] for r in turn['tool_results'] if r['tool_name']=='request_tool_scope' and r['status']=='completed']
    assert scopes[-1]['source_selection_required'] is True,scopes[-1]
    assert len(re.findall(r'^\s*\d+[.)]\s+',response['text'],flags=re.M))>=2,response
    assert not response['artifacts'] and elapsed<90000 and not live_web[2],result

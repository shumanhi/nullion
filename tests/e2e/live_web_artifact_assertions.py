"""Native Web save assertions and explicitly labeled saved-boundary reuse.

Replay opens the exact owned conversation through default history. It never
submits a chat turn, regenerates a file, or sends a cleanup /stop request.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urljoin

import pytest


def save_artifact_through_ui(page, base, artifact, control):
    from playwright.sync_api import expect
    original_name=Path(artifact.get('name') or artifact.get('filename') or '').name
    url=urljoin(base,artifact['url'])
    with page.expect_response(lambda response: response.request.method=='POST' and response.url==url+'/save') as pending:
        control.click()
    response=pending.value
    assert response.ok,response.text()
    receipt=response.json()
    assert receipt.get('ok') is True and receipt.get('name') and receipt.get('path'),receipt
    assert Path(receipt['path']).is_absolute() and Path(receipt['path']).name==receipt['name'],receipt
    if original_name:
        assert Path(receipt['name']).suffix==Path(original_name).suffix,receipt
        assert receipt['name']==original_name or receipt['name'].startswith(Path(original_name).stem+' ('),receipt
    expect(control).to_have_text('Saved '+receipt['name'])
    expect(control).to_have_attribute('title',receipt['path'])
    assert 'downloaded' in control.get_attribute('class').split()
    source=page.request.get(url)
    assert source.ok,source.text()
    body=source.body()
    assert receipt.get('bytes')==len(body)>0,receipt
    return body,{**receipt,'source_sha256':hashlib.sha256(body).hexdigest()}


def pdf_source_phrase_visible(expected, logical_text, raw_page_text):
    """Use logical text first; allow a source dash token to wrap physically.

    Literal spaces are never removed. Only a newline immediately after an
    existing dash inside the expected source token, plus its indentation, may
    join that token's two rendered pieces.
    """
    import re
    import unicodedata
    from scripts.live_stage_chat_contract_probe import _normalized_visible_text
    phrase=_normalized_visible_text(expected)
    if phrase in _normalized_visible_text(logical_text):
        return True
    parts=[];wrapped_dash=False
    for index,character in enumerate(phrase):
        parts.append(r"\s+" if character.isspace() else re.escape(character))
        if (unicodedata.category(character)=='Pd' and index>0 and index+1<len(phrase)
                and not phrase[index-1].isspace() and not phrase[index+1].isspace()):
            parts.append(r"(?:\r?\n[ \t]*)?")
            wrapped_dash=True
    if not wrapped_dash:
        return False
    return re.search(''.join(parts),raw_page_text) is not None


def selected_artifact_conversation_script(conversation):
    # This is the app's existing explicit-selection flag. Without it startup
    # intentionally chooses the latest active conversation, not this saved id.
    return ("localStorage.setItem('nullion_conv_id',"+json.dumps(conversation)+
        ");localStorage.setItem('nullion_chat_restore_suppressed','true');")


def retained_plain_latencies(replay):
    filename=replay.get('latency_frames_path')
    assert filename,'Saved capture reuse requires the original frame evidence for plain-turn timing'
    path=Path(filename)
    if not path.is_absolute():path=Path(__file__).resolve().parents[2]/path
    raw=path.read_bytes()
    rows=[json.loads(line) for line in raw.splitlines() if line.strip()]
    submitted=[row for row in rows if row['frame'].get('type')=='_submitted'
        and row['frame'].get('conversation_id')==replay['conversation_id']]
    samples=[]
    for item in submitted:
        identity=item['frame']['turn_id']
        if identity==replay['turn_id']:break
        terminal=[row for row in rows if row['frame'].get('type')=='done'
            and row['frame'].get('turn_id')==identity
            and row['frame'].get('conversation_id',replay['conversation_id'])==replay['conversation_id']
            and not row['frame'].get('superseded')]
        assert len(terminal)==1 and not terminal[0]['frame'].get('artifacts'),terminal
        samples.append((terminal[0]['received_at']-item['received_at'])*1000)
    assert len(samples)==3 and all(value>=0 for value in samples),samples
    return samples,{'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'evidence_kind':'retained_generation_timing'}


@contextmanager
def artifact_web_session(request, replay_environment):
    raw=os.environ.get(replay_environment)
    if not raw:
        yield SimpleNamespace(surface=request.getfixturevalue('live_web'),replay=None,saved=None)
        return
    from playwright.sync_api import sync_playwright
    from tests.e2e.test_live_stage_web_matrix import _turns,watch_chat_frames
    replay=json.loads(raw)
    assert isinstance(replay,dict)
    conversation=replay.get('conversation_id');identity=replay.get('turn_id')
    assert isinstance(conversation,str) and conversation.startswith('web:live-ui-audit:'),replay
    assert isinstance(identity,str) and identity,replay
    assert os.environ.get('NULLION_E2E_CONTAINER')=='1','Docker required'
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753')
    assert base=='http://127.0.0.1:8753','Stage-only loopback forwarder required'
    database=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB','/stage/runtime.db'))
    original_turns=_turns(database,conversation)
    original_ids={turn.get('turn_id') for turn in original_turns}
    native=[turn for turn in original_turns if turn.get('turn_id')==identity]
    assert len(native)==1 and native[0].get('response_fulfilled') is True,native
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        context=browser.new_context()
        context.add_init_script(selected_artifact_conversation_script(conversation))
        page=context.new_page();frames=[];errors=[];posts=[]
        watch_chat_frames(page,frames,errors,base)
        page.on('request',lambda item:posts.append(item.url) if item.method=='POST' and item.url==base+'/api/chat' else None)
        try:
            page.goto(base,wait_until='domcontentloaded')
            page.locator('#user-input').wait_for(state='visible')
            history=page.request.get(base+'/api/chat/history/'+conversation)
            assert history.ok,history.text()
            messages=[row for row in history.json()['messages'] if row.get('role')=='bot'
                and row.get('metadata',{}).get('runtime_turn_id')==identity]
            assert len(messages)==1 and messages[0].get('artifacts'),messages
            saved={'text':messages[0]['text'],'artifacts':messages[0]['artifacts']}
            replay={**replay,'evidence_kind':'saved_boundary_reuse','fresh_generation':False}
            yield SimpleNamespace(surface=(page,frames,errors,database,conversation,base),replay=replay,
                saved=(saved,native[0],None))
            assert not posts and not any(frame.get('type')=='_submitted' for frame in frames), 'Saved-boundary reuse must never submit a model/user request'
            assert {turn.get('turn_id') for turn in _turns(database,conversation)}==original_ids, 'Saved-boundary reuse appended a native turn'
        finally:
            output=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))/'saved-artifact-reuse'
            output.mkdir(parents=True,exist_ok=True)
            (output/(identity.replace(':','_')+'.json')).write_text(json.dumps({'replay':replay,'posts':posts,'page_errors':errors},indent=2))
            page.screenshot(path=str(output/(identity.replace(':','_')+'.png')),full_page=False)
            browser.close()


@pytest.fixture
def capture_web(request):
    with artifact_web_session(request,'NULLION_LIVE_CAPTURE_REPLAY') as state:yield state


@pytest.fixture
def pdf_web(request):
    with artifact_web_session(request,'NULLION_LIVE_PDF_REPLAY') as state:yield state

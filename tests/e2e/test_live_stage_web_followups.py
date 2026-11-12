"""Additional real Web follow-ups using task-owned stage objects only."""
import json
import os
from pathlib import Path
import time
import statistics
from uuid import uuid4

import pytest

from tests.e2e.test_live_stage_web_matrix import catalogue, live_web, _submit

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_LIVE_STAGE_E2E") != "1",
                               reason="Opt-in real stage Web acceptance")


def test_live_web_command_named_file_uses_shared_attachment_delivery(live_web):
    import csv,io
    from decimal import Decimal
    from playwright.sync_api import expect
    page,frames,errors,database,conversation,base=live_web
    root=Path(os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT'])
    filename='stage 共通 '+uuid4().hex[:10]+'.csv';source=root/filename
    source.write_text('Region,Value\nNorth,1\n')
    prompt=f'Update {source} with CSV columns Region,Value and exactly two data rows: North 12 and South 19. Attach only this CSV, keeping its complete filename.'
    try:
        response,turn,elapsed=_submit(live_web,'/chat '+prompt,timeout=180,audit_prompt=prompt)
        assert turn.get('response_fulfilled') is True,turn
        items=response.get('artifacts',[])
        assert len(items)==1 and items[0]['name']==filename,response
        downloaded=page.request.get(base+items[0]['url']).body()
        rows=list(csv.reader(io.StringIO(downloaded.decode('utf-8-sig'))))
        assert rows[0]==['Region','Value'] and len(rows)==3,rows
        assert [(r[0],Decimal(r[1])) for r in rows[1:]]==[('North',Decimal(12)),('South',Decimal(19))],rows
        assert source.read_bytes()==downloaded
        links=page.locator('#messages a.artifact-link[download]')
        expect(links).to_have_count(1);expect(links).to_be_visible()
        history=page.request.get(base+'/api/chat/history/'+conversation).json()['messages']
        attached=[a for row in history for a in (row.get('metadata') or {}).get('artifacts',[])
                  if (a.get('name') or Path(a.get('path','')).name)==filename]
        assert len(attached)==1,history
        page.reload(wait_until='domcontentloaded')
        expect(page.locator('#messages a.artifact-link[download]')).to_have_count(1)
        assert not errors
    finally:
        source.unlink(missing_ok=True)


def test_live_web_owned_scheduler_rename_and_cancel(live_web):
    page, frames, errors, database, conversation, base = live_web
    marker = "Web followup " + uuid4().hex[:8]
    created, _ = catalogue._post_json(base + "/api/crons", {
        "name": marker, "task": "Read-only synthetic status. Reply with ready.",
        "schedule": "0 12 * * *", "timezone": "UTC", "enabled": False,
        "delivery_channel": "web", "delivery_target": conversation,
    }, timeout=30)
    job = created["job"]
    out = Path(os.environ.get("NULLION_LIVE_EVIDENCE_DIR", "/tmp/live-web-evidence")) / "scheduler_followup"
    out.mkdir(parents=True, exist_ok=True)
    try:
        response, turn, elapsed = _submit(live_web,
            f'Renombra únicamente la tarea pausada "{marker}" ({job["id"]}) a "{marker} revisado". '
            'Mantén su horario y no la ejecutes ni la actives. '
            'Si después necesitas una aclaración para cancelarla, ofrece 1. Eliminarla permanentemente, '
            '2. Mantenerla pausada. Espera mi elección antes de eliminarla.')
        rows = catalogue._get_json(base + "/api/crons", timeout=30)
        changed = next(row for row in rows if row["id"] == job["id"])
        assert changed["name"] == marker + " revisado" and not changed["enabled"], changed
        # The referent is the task just changed, without restating its ID.
        before = len(frames)
        page.locator("#user-input").fill("Cancel it.")
        page.locator("#user-input").press("Enter")
        deadline = time.monotonic() + 90
        deleted = False
        approved = False
        chose_deletion = False
        while time.monotonic() < deadline:
            page.wait_for_timeout(200)
            approvals = [f for f in frames[before:] if f.get("type") == "approval_required"]
            if approvals and not approved:
                approval = approvals[-1]
                assert job["id"] in json.dumps(approval) or marker in json.dumps(approval), approval
                # Approve only the deletion of the fixture this test owns.
                page.get_by_role("button", name="Allow once", exact=True).last.click()
                approved = True
            terminals = [f for f in frames[before:] if f.get('type') == 'done']
            if terminals and not chose_deletion and not approved:
                chunks = ''.join(f.get('text', '') for f in frames[before:]
                                 if f.get('type') == 'chunk' and f.get('turn_id') == terminals[-1].get('turn_id'))
                # Cancelling an already-paused task can validly require a
                # deletion choice. Its referent must still be this exact job.
                rows = catalogue._get_json(base + '/api/crons', timeout=15)
                if any(row['id'] == job['id'] for row in rows):
                    assert marker + ' revisado' in chunks and '1.' in chunks and '2.' in chunks, chunks
                    before = len(frames)
                    page.locator('#user-input').fill('1')
                    page.locator('#user-input').press('Enter')
                    chose_deletion = True
                    deadline = time.monotonic() + 90
            rows = catalogue._get_json(base + "/api/crons", timeout=15)
            if not any(row["id"] == job["id"] for row in rows):
                deleted = True
                break
        assert deleted, page.locator("#messages").inner_text()
        assert not errors
        page.screenshot(path=str(out / "web.png"), full_page=False, timeout=10000)
        (out / "result.json").write_text(json.dumps({"status":"passed", "conversation_id":conversation,
            "owned_cron_id":job["id"], "rename_turn":turn, "numeric_choice":chose_deletion, "deleted":True}, indent=2))
    finally:
        rows = catalogue._get_json(base + "/api/crons", timeout=30)
        if any(row["id"] == job["id"] for row in rows):
            catalogue._delete_json(base + "/api/crons/" + job["id"], timeout=30)


def test_live_web_generated_csv_has_one_delivery_and_reload_keeps_it(live_web):
    page, frames, errors, database, conversation, base = live_web
    marker = uuid4().hex[:8]
    filename = "dedupe_" + marker + ".csv"
    response, turn, elapsed = _submit(live_web,
        f'Create one CSV named {filename} with columns Region and Value and these rows: North 12, South 19. '
        'Deliver only the final CSV. Keep any scripts or internal state private.')
    assert len(response["artifacts"]) == 1, response
    artifact = response["artifacts"][0]
    data, content_type = catalogue._artifact_bytes(base, artifact, timeout=30)
    assert b"North" in data and b"12" in data and b"South" in data and b"19" in data
    links = page.locator('#messages a[download]').filter(has_text=filename)
    assert links.count() == 1
    page.reload(wait_until="domcontentloaded")
    from playwright.sync_api import expect
    expect(page.locator('#messages a[download]').filter(has_text=filename)).to_have_count(1, timeout=15000)
    assert not errors


def test_live_web_plain_turn_latency_across_languages(live_web):
    _, frames, _, _, _, _ = live_web
    samples = []
    for prompt, expected in (("Return only the number: 2+2", "4"),
                             ("Devuelve solo el número: 3+2", "5"),
                             ("数字だけで答えてください：4+2", "6")):
        before = len(frames)
        response, turn, elapsed = _submit(live_web, prompt, timeout=30)
        assert response["text"].strip() == expected, response
        assert not response["artifacts"] and not turn.get("tool_results")
        assert not [frame for frame in frames[before:] if frame.get("tool_name")]
        samples.append(elapsed)
    assert statistics.mean(samples) < 9000 and max(samples) < 15000, samples


def test_live_web_conceptual_question_does_not_end_with_shell_receipt(live_web):
    response, turn, elapsed = _submit(live_web,
        "What are the privacy concerns with connecting Gemini to Gmail? Explain what data is shared, "
        "account permissions, and ways to reduce exposure. Do not access my mailbox or change settings.")
    assert len(response["text"]) > 200, response
    assert not response["artifacts"]
    names = {result["tool_name"] for result in turn.get("tool_results", [])}
    assert not names.intersection({"terminal_exec", "email_search", "email_read", "email_send"}), names
    assert "Local terminal capability verified" not in response["text"]


def test_live_web_archived_history_recall_from_fresh_conversation(live_web):
    page, frames, errors, database, conversation, base = live_web
    token = uuid4().hex[:10]
    topic, code = "Cedar-" + token, "REF-" + uuid4().hex[:10]
    _submit(live_web, f'For our synthetic {topic} parcel, the reference is {code}. '
            'Earlier status was shipped. Acknowledge only; no external lookup or durable preference.')
    page.locator("#composer-archive-btn").click()
    page.get_by_role("button", name="Archive", exact=True).click()
    from playwright.sync_api import expect
    expect(page.locator("#messages")).not_to_contain_text(code)
    new_conversation = page.evaluate("localStorage.getItem('nullion_conv_id')")
    assert new_conversation and new_conversation != conversation
    fresh = page, frames, errors, database, new_conversation, base
    response, turn, elapsed = _submit(fresh,
        f'What reference did we previously record for {topic}? Use saved history. '
        'Do not infer its present delivery status or access external accounts.')
    assert code in response["text"], response
    assert "chat_history_search" in {row["tool_name"] for row in turn.get("tool_results", [])}, turn
    assert not response["artifacts"]
    assert not {"email_search", "email_read", "calendar_list"}.intersection(
        row["tool_name"] for row in turn.get("tool_results", []))


def test_live_web_owned_cron_delivers_report_with_actual_data(live_web):
    page, frames, errors, database, conversation, base = live_web
    token = uuid4().hex[:10]
    filename = "scheduled_" + token + ".html"
    created, _ = catalogue._post_json(base + "/api/crons", {
        "name":"Web report " + token, "schedule":"0 0 1 1 *", "enabled":False,
        "delivery_channel":"web", "delivery_target":conversation,
        "task":f'Create one self-contained HTML report named {filename}. Include a table '
               f'with North=12 and South=19, total=31, and the marker {token}. '
               'Deliver only the finished HTML. No external lookups are needed.',
        "artifact_delivery_options":{"required_artifact_extensions":[".html"],
            "requires_attachment_delivery":True,"html_image_delivery_mode":"file",
            "doctor_monitoring_enabled":False},
    }, timeout=30)
    identifier = created["job"]["id"]
    try:
        _submit(live_web, f'Run only scheduled task {identifier} once now. Keep it paused.', timeout=240)
        from playwright.sync_api import expect
        expect(page.locator('#messages a[download]').filter(has_text=filename)).to_have_count(1, timeout=240000)
        history = catalogue._get_json(base + "/api/chat/history/" + conversation, timeout=30)
        artifacts = [artifact for message in history["messages"] for artifact in message.get("artifacts", [])
                     if Path(catalogue._artifact_name(artifact)).name == filename]
        assert len(artifacts) == 1, history
        data, content_type = catalogue._artifact_bytes(base, artifacts[0], timeout=30)
        assert all(value in data for value in (token.encode(), b"North", b"South", b"12", b"19", b"31")), data
        deadline = time.monotonic() + 15
        while True:
            jobs = catalogue._get_json(base + "/api/crons", timeout=30)
            stored = next(row for row in jobs if row["id"] == identifier)
            if stored["last_result"] == "ok" or time.monotonic() >= deadline:
                break
            page.wait_for_timeout(100)
        assert not stored["enabled"] and stored["last_result"] == "ok", stored
        assert not errors
    finally:
        catalogue._delete_json(base + "/api/crons/" + identifier, timeout=30)


def test_live_web_archive_extract_transformation_returns_only_final_file(live_web):
    token = uuid4().hex[:8]
    source, _, _ = _submit(live_web,
        f'Create a ZIP named source_{token}.zip containing two CSV files: north.csv with columns '
        'Region,Value and row North,12; south.csv with the same columns and row South,19. '
        'Deliver only the ZIP.')
    assert len(source["artifacts"]) == 1
    filename = "combined_" + token + ".csv"
    result, turn, _ = _submit(live_web,
        f'Extract the ZIP just created in this conversation and combine its two CSV files into '
        f'{filename}. Keep the two data rows and add a Total row with 31. Attach only the final CSV, '
        'without the archive, extracted files or internal notes.')
    assert len(result["artifacts"]) == 1, result
    assert Path(catalogue._artifact_name(result["artifacts"][0])).name == filename
    data, _ = catalogue._artifact_bytes(live_web[-1], result["artifacts"][0], timeout=30)
    assert all(value in data for value in (b"North", b"South", b"Total", b"31")), data
    completed = {row["tool_name"] for row in turn.get("tool_results", []) if row["status"] == "completed"}
    # Dedicated extraction is preferred; a permitted core execution fallback
    # can also finish the same user-visible transformation.
    assert completed.intersection({"archive_extract", "terminal_exec"}), completed


def test_live_web_browser_report_synthesis_preserves_helmet_answer(live_web):
    result, turn, elapsed = _submit(live_web,
        'Research NYC Citi Bike e-bike helmet requirements for adults. First call request_tool_scope with capabilities [web], tool_names [browser_run_task], and required_tool_names [browser_run_task]. Then use browser_run_task to read '
        'https://citibikenyc.com/how-it-works/electric and official NYC guidance as needed. '
        'Give a concise answer distinguishing provider advice from legal requirements, with source links. '
        'Do not create or attach files.', timeout=720)
    assert "helmet" in result["text"].lower() and "citi" in result["text"].lower(), result
    assert not result["artifacts"]
    reports=[row["output"] for row in turn.get("tool_results", [])
             if row["tool_name"]=="browser_run_task" and row["status"]=="completed"]
    assert reports and any(report.get("observations") for report in reports),turn
    from urllib.parse import urlsplit
    assert any(isinstance(index:=observation.get("source_index"),int)
        and 0<=index<len(report.get("sources",[]))
        and urlsplit(report["sources"][index]["source_url"]).hostname=="citibikenyc.com"
        for report in reports for observation in report.get("observations",[])),reports
    assert "https://citibikenyc.com/" in result["text"],result


def _owned_browser_jobs(database, conversation):
    connection=os.environ.get('NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION')
    if connection:
        from urllib.request import Request,urlopen
        from urllib.parse import urlencode
        settings=json.loads(Path(connection).read_text())
        request=Request('http://host.docker.internal:'+str(settings['port'])+'/browser-jobs?'+
            urlencode({'conversation_id':conversation}),headers={'Authorization':'Bearer '+settings['token']})
        from scripts.live_stage_http import read_json
        payload=read_json(request,opener=urlopen)
        assert payload['conversation_id']==conversation
        return payload['jobs']
    jobs=[]
    for path in (database.parent/'browser-task-scratch').glob('run-*/job.json'):
        try:
            job=json.loads(path.read_text())
            if (job.get('owner') or {}).get('conversation_id')==conversation:
                jobs.append({'resource_id':path.parent.name,'worker_log_bytes':(path.parent/'worker.log').stat().st_size})
        except (OSError,ValueError):pass
    return jobs


def test_live_web_stop_releases_owned_browser_and_next_lookup_completes(live_web):
    page, frames, errors, database, conversation, base = live_web
    prompt = ('Use browser_run_task on https://books.toscrape.com/ to compare ten books across '
              'three catalogue pages, recording displayed prices and stock. Return source-backed findings. '
              'Do not create files or make purchases.')
    page.locator('#user-input').fill(prompt); page.locator('#user-input').press('Enter')
    scratch = database.parent / 'browser-task-scratch'
    owned = None
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline:
            page.wait_for_timeout(100)
            owned = next((job['resource_id'] for job in _owned_browser_jobs(database,conversation)
                          if job['worker_log_bytes']>0),None)
            if owned: break
        assert owned, 'The real isolated browser worker never started for this Web request'
        before = len(frames)
        page.locator('#user-input').fill('/stop'); page.locator('#user-input').press('Enter')
        deadline = time.monotonic() + 15
        stopped = None
        while time.monotonic() < deadline:
            page.wait_for_timeout(100)
            stopped = next((frame for frame in frames[before:]
                if frame.get('cancelled_turn_ids') and frame.get('type') in {'session_stopped','done'}), None)
            if stopped and not any(job['resource_id']==owned for job in _owned_browser_jobs(database,conversation)): break
        assert stopped and stopped.get('cancelled_turn_ids'), frames[before:]
        assert not any(job['resource_id']==owned for job in _owned_browser_jobs(database,conversation)), 'Cancelled browser still holds its source/session resources'
        from playwright.sync_api import expect
        expect(page.locator('#messages')).to_contain_text('Stopped', timeout=10000)
        result, _, _ = _submit(live_web, 'Return only the number: 2+2', timeout=30)
        assert result['text'].strip() == '4' and not result['artifacts']
        result, turn, _ = _submit(live_web,
            'Use browser_run_task to read the displayed price and copies in stock at '
            'https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html. '
            'Return the two values and source URL only. No files.', timeout=120)
        assert '51.77' in result['text'] and '22' in result['text'], result
        assert any(row['tool_name']=='browser_run_task' and row['status']=='completed'
                   for row in turn.get('tool_results', [])), turn
    finally:
        # Cleanup is scoped to this UUID-owned conversation even if worker
        # discovery failed, or the UI switched to HTTP fallback transport.
        page.request.post(base+'/api/chat',data={'text':'/stop','conversation_id':conversation},timeout=30000)


def test_live_web_single_page_sdk_lookup_returns_verified_values(live_web):
    result,turn,_=_submit(live_web,
        'Use browser_run_task to read the displayed price and copies in stock at '
        'https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html. '
        'Return the two values and source URL only. No files.', timeout=360)
    assert '51.77' in result['text'] and '22' in result['text'] and not result['artifacts'],result
    assert any(row['tool_name']=='browser_run_task' and row['status']=='completed'
               for row in turn.get('tool_results',[])),turn


def test_live_web_email_pagination_follows_exact_continuation_without_query_partitioning(live_web):
    result,turn,_=_submit(live_web,
        'Read only the first two pages of my connected inbox using email_search with query in:inbox '
        'and limit 2. For page two use the exact next_page_request arguments returned by page one. '
        'Summarize only those four message headers, newest first; explicitly limit the summary to these two pages. '
        'Do not read full bodies, open links, mark messages read, send email or create files.',timeout=120)
    reads=[row for row in turn.get('tool_results',[]) if row['tool_name']=='email_search' and row['status']=='completed']
    assert len(reads)==2,[(row['tool_name'],row['status']) for row in turn.get('tool_results',[])]
    first,second=(row['output'] for row in reads)
    assert first['query']==second['query']=='in:inbox' and first['limit']==second['limit']==2
    assert first['source_pagination']['scope_id']==second['source_pagination']['scope_id']
    assert second['source_pagination']['cursor']==first['next_page_request']['arguments']['page_token']
    assert sum(row['result_count'] for row in (first,second))==4
    assert not any(row['tool_name'] in {'email_read','email_send','calendar_create','file_write'} for row in turn['tool_results'])
    assert result['text'].strip() and not result['artifacts']


def test_live_web_browser_facts_can_become_a_generated_visual(live_web):
    """A sourced fact is compatible with an explicitly generated illustration."""
    response, turn, _ = _submit(live_web,
        'Lee con browser_run_task el precio y las existencias de '
        'https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html. '
        'Después usa image_generate para crear una infografía con esos dos valores. '
        'Una imagen SVG local es aceptable. Son datos para una ilustración nueva, '
        'no fotografías del libro. Entrega una sola imagen en esta conversación; '
        'no programes tareas ni envíes mensajes externos.', timeout=420)
    completed = {row['tool_name'] for row in turn.get('tool_results', []) if row['status'] == 'completed'}
    assert 'browser_run_task' in completed and completed.intersection({'image_generate','file_write'}), turn
    assert len(response['artifacts']) == 1, response
    image = response['artifacts'][0]
    data, content_type = catalogue._artifact_bytes(live_web[-1], image, timeout=30)
    assert data and (content_type.startswith('image/') or
        Path(catalogue._artifact_name(image)).suffix.lower() in {'.svg', '.png', '.jpg', '.webp'}), content_type
    if Path(catalogue._artifact_name(image)).suffix.lower() == '.svg':
        assert b'51.77' in data and b'22' in data, data[:1200]
    assert not any(row['status'] == 'failed' and (row.get('output') or {}).get('reason') ==
        'sourced_image_collection_failed' for row in turn.get('tool_results', [])), turn


def test_live_web_browser_facts_do_not_block_raster_image_generation(live_web):
    config=catalogue._get_json(live_web[-1]+'/api/config',timeout=15)
    if not config.get('image_generate_enabled') or not config.get('image_generate_available'):
        pytest.skip('Stage image_generate is disabled or unavailable; preserve its configured media settings')
    response, turn, _ = _submit(live_web,
        'Use browser_run_task to read the displayed price and exact stock count at '
        'https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html. '
        'Then call image_generate with a .png output_path to create one simple infographic '
        'showing these two values in large legible text. Deliver the generated PNG here. '
        'This is a newly generated illustration of numerical facts; do not use source photographs, '
        'write SVG code, schedule anything, or send external messages.',timeout=420)
    completed={row['tool_name'] for row in turn.get('tool_results',[]) if row['status']=='completed'}
    assert {'browser_run_task','image_generate'} <= completed,turn
    generated=[row.get('output',{}) for row in turn.get('tool_results',[])
        if row.get('tool_name')=='image_generate' and row.get('status')=='completed']
    assert generated and any(not row.get('fallback_used') for row in generated),generated
    assert len(response['artifacts'])==1,response
    artifact=response['artifacts'][0]
    data,content_type=catalogue._artifact_bytes(live_web[-1],artifact,timeout=30)
    assert data.startswith(b'\x89PNG\r\n\x1a\n'),content_type
    from PIL import Image
    import io
    with Image.open(io.BytesIO(data)) as image:
        assert image.width>=256 and image.height>=256,image.size
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'browser-facts-image'
    out.mkdir(parents=True,exist_ok=True)
    (out/'infographic.png').write_bytes(data)
    (out/'result.json').write_text(json.dumps({'reply':response,'turn':turn},ensure_ascii=False,indent=2))
    assert not any(row['status']=='failed' and (row.get('output') or {}).get('reason')==
        'sourced_image_collection_failed' for row in turn.get('tool_results',[])),turn


def test_live_web_local_read_does_not_append_activity_when_verbose_is_off(live_web):
    from scripts.live_stage_chat_contract_probe import _stage_workspace_upload_root
    page,frames,errors,database,conversation,base=live_web
    setting=page.request.post(base+'/api/chat',data={'text':'/verbose off','conversation_id':conversation})
    assert setting.ok,setting.text()
    value='READ-'+uuid4().hex[:10]
    source=_stage_workspace_upload_root()/('verbose-'+uuid4().hex+'.txt')
    source.write_text(value+'\n')
    try:
        response,turn,_=_submit(live_web,f'Lee {source}. Responde solo con el valor exacto, '
            'sin formato adicional. No cambies archivos ni envíes adjuntos.',timeout=120)
        assert value in response['text'] and not response['artifacts'],response
        assert 'Activity\n' not in response['text'],response
        assert response['text'].strip()==turn['assistant_reply'].strip(),turn
        assert any(row['tool_name']=='file_read' and row['status']=='completed'
            for row in turn.get('tool_results',[])),turn
        from playwright.sync_api import expect
        bubble=page.locator('#messages .msg.bot .bubble').filter(has_text=value)
        expect(bubble).to_have_count(1)
        assert 'Activity\n' not in bubble.get_attribute('data-raw-text')
        page.reload();expect(bubble).to_have_count(1)
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'verbose-off-read'
        out.mkdir(parents=True,exist_ok=True)
        (out/'result.json').write_text(json.dumps({'reply':response,'turn':turn},ensure_ascii=False,indent=2))
        page.screenshot(path=str(out/'web.png'),full_page=False)
    finally:source.unlink(missing_ok=True)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_IMAGE_REJECTION_STATUS'),
    reason='Opt-in real provider rejection; requires a known unavailable provider')
def test_live_web_rejected_image_provider_does_not_publish_a_placeholder(live_web):
    expected = int(os.environ['NULLION_LIVE_IMAGE_REJECTION_STATUS'])
    response, turn, _ = _submit(live_web,
        'Generate a PNG image of a monkey walking toward a lake house and attach it here. '
        'Use the configured image model. If the model rejects generation, explain the observed '
        'failure and what I need to fix; do not substitute a prompt card or placeholder.', timeout=180)
    rejected = [r for r in turn.get('tool_results', []) if r.get('tool_name') == 'image_generate'
        and r.get('status') == 'failed' and r.get('output', {}).get('http_status_code') == expected]
    assert rejected, turn
    assert response.get('text', '').strip() and not response.get('artifacts'), response
    assert str(expected) in response['text'], response
    assert turn.get('response_fulfilled') is not True, turn
    assert not any(r.get('output', {}).get('fallback_used') for r in turn.get('tool_results', [])), turn
    out = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'provider-rejection.json'
    out.write_text(json.dumps({'reply':response,'turn':turn},ensure_ascii=False,indent=2))

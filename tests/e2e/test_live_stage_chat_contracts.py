from __future__ import annotations

import json
import os
from pathlib import Path

import pytest


@pytest.mark.skipif(
    os.environ.get("NULLION_LIVE_STAGE_E2E") != "1",
    reason="live stage chat probes use real running services and provider/tool quota",
)
def test_live_stage_chat_tool_scope_and_latency_contracts() -> None:
    from scripts.live_stage_chat_contract_probe import DEFAULT_LIVE_STAGE_SCENARIOS, main

    base_url = os.environ.get("NULLION_LIVE_BASE_URL", "http://127.0.0.1:8753")
    runtime_db = os.environ.get("NULLION_LIVE_RUNTIME_DB", str(Path.home() / ".nullion-stage" / "runtime.db"))
    repeat = os.environ.get("NULLION_LIVE_REPEAT", "2")
    scenarios = os.environ.get(
        "NULLION_LIVE_SCENARIOS",
        ",".join(DEFAULT_LIVE_STAGE_SCENARIOS),
    )
    argv = [
        "--base-url",
        base_url,
        "--runtime-db",
        runtime_db,
        "--repeat",
        repeat,
    ]
    for scenario in (item.strip() for item in scenarios.split(",")):
        if scenario:
            argv.extend(["--scenario", scenario])

    assert main(argv) == 0


@pytest.mark.skipif(
    os.environ.get("NULLION_LIVE_STAGE_E2E") != "1",
    reason="requires an authorized deployment and real stage provider/tool quota",
)
def test_live_stage_flight_correction_keeps_parent_and_delivers_final_pdf() -> None:
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe

    base_url = os.environ.get("NULLION_LIVE_BASE_URL", "http://127.0.0.1:8753").rstrip("/")
    runtime_db = Path(os.environ.get("NULLION_LIVE_RUNTIME_DB", str(Path.home() / ".nullion-stage" / "runtime.db")))
    assert runtime_db.exists(), "The live contract requires backing runtime evidence"
    # Ordinary latency and an adjacent tool family must remain reliable too.
    assert probe.main(["--base-url", base_url, "--runtime-db", str(runtime_db),
        "--scenario", "simple", "--scenario", "weather", "--repeat", "2"]) == 0
    conversation_id = f"web:live-stage-flight-correction:{uuid4().hex}"
    parent = (
        "Can you search couple of direct flights to see which one might be cheaper for a weekend "
        "this month (except the next one) ..leave wed..come back Sunday. Nyc to SF create a pdf for easy delivery"
    )
    correction = "But there are more weekends in this month"
    parent_response, _ = probe._post_json(f"{base_url}/api/chat",
        {"text": parent, "conversation_id": conversation_id}, timeout=300)
    # The Web API does not perform Telegram's Doctor budget continuation.
    # Extend only this read-only research run, never a tool/write approval.
    for _ in range(2):
        if parent_response.get("type") != "approval_required":
            break
        assert parent_response.get("request_kind") == "agent_turn_limit_extension", parent_response
        parent_response = probe._approve_once(base_url, parent_response["approval_id"], 600, mode="limit_2x")
    assert parent_response.get("type") != "approval_required", parent_response
    parent_event = probe._chat_turn_for_user_message_with_poll(runtime_db, conversation_id, parent)
    assert parent_event and parent_event.get("turn_id"), parent_response
    response, elapsed_ms = probe._post_json(f"{base_url}/api/chat",
        {"text": correction, "conversation_id": conversation_id}, timeout=180)
    event = probe._chat_turn_for_user_message_with_poll(runtime_db, conversation_id, correction)
    assert event and event.get("parent_turn_id") == parent_event["turn_id"], event
    scenario = probe.Scenario(
        "flight_correction", correction, None, 180000,
        forbidden_text=("enough verified evidence", "task is still open", "couldn't attach"),
        required_tool_any=("web_search", "web_fetch", "browser_navigate"),
        required_tool_all=("pdf_create",),
        forbidden_tool_any=("list_crons", "add_cron", "update_cron", "toggle_cron", "delete_cron", "run_cron"),
        required_artifact_extensions=(".pdf",), min_artifact_count=1,
    )
    probe._assert_contract(scenario, response=response, elapsed_ms=elapsed_ms,
        tool_names=probe._tool_names_from_chat_turn(event), runtime_db_available=True,
        tool_output_texts=[json.dumps(result) for result in event.get("tool_results", [])])
    # Check only this final response, so the original PDF cannot mask a failed correction.
    downloaded = probe._download_and_check_artifacts(base_url, response.get("artifacts") or [],
        scenario=scenario, timeout=30)
    assert any(artifact["name"].lower().endswith(".pdf") and artifact["visible_text_chars"] > 100
        for artifact in downloaded), downloaded
    # A delivered PDF with missing prices was the observed failure. Byte count
    # and a completed source call are transport evidence, not comparison proof.
    import io
    import re
    from pypdf import PdfReader
    pdfs = [artifact for artifact in response.get("artifacts") or []
        if probe._artifact_name(artifact).lower().endswith(".pdf")]
    assert pdfs
    for artifact in pdfs:
        data, _ = probe._artifact_bytes(base_url, artifact, timeout=30)
        reader = PdfReader(io.BytesIO(data))
        visible = "\n".join(page.extract_text() or "" for page in reader.pages)
        assert len(re.findall(r"(?:USD\s*|\$)\s*\d+(?:[.,]\d+)*", visible)) >= 2, visible
        assert "Unavailable in verified search output" not in visible
        assert "Not returned by available search evidence" not in visible
        assert "##" not in visible and "**" not in visible
    # The source-backed completion decision must cover the request, rather
    # than a file receipt silently bypassing it. This API probe is diagnostic;
    # live acceptance still requires the real Web UI and Telegram app paths.
    reviews = probe._conversation_event_payloads(runtime_db, conversation_id,
        event_type="conversation.completion_review")
    assert any(review.get("disposition") == "complete" and review.get("unresolved_count") == 0 for review in reviews), reviews


def test_live_stage_default_matrix_covers_core_tool_families() -> None:
    from scripts.live_stage_chat_contract_probe import (
        DEEP_LIVE_STAGE_SCENARIOS,
        DEFAULT_LIVE_STAGE_SCENARIOS,
        SCENARIOS,
    )

    selected = [SCENARIOS[name] for name in (*DEFAULT_LIVE_STAGE_SCENARIOS, *DEEP_LIVE_STAGE_SCENARIOS)]
    covered_tools = {
        tool
        for scenario in selected
        for tool in (*scenario.required_tool_any, *scenario.required_tool_all)
    }

    assert set(DEFAULT_LIVE_STAGE_SCENARIOS).issubset(SCENARIOS)
    assert set(DEEP_LIVE_STAGE_SCENARIOS).issubset(SCENARIOS)
    assert {
        "archive_extract",
        "browser_navigate",
        "calendar_list",
        "document_create",
        "email_send",
        "email_read",
        "email_search",
        "file_download",
        "file_search",
        "file_write",
        "image_generate",
        "list_crons",
        "market_quote",
        "pdf_create",
        "presentation_create",
        "run_cron",
        "spreadsheet_create",
        "terminal_exec",
        "toggle_cron",
        "weather_forecast",
    }.issubset(covered_tools)
    assert "email_send_approval_payload_live" in SCENARIOS
    assert SCENARIOS["email_send_approval_payload_live"].requires_email_send_flag is True


@pytest.mark.skipif(
    os.environ.get('NULLION_LIVE_STAGE_E2E') != '1',
    reason='requires authorized stage validation and real provider/tool quota',
)
def test_live_stage_text_lookup_keeps_browser_evidence_internal() -> None:
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe

    base_url = os.environ.get('NULLION_LIVE_BASE_URL', 'http://127.0.0.1:8753').rstrip('/')
    runtime_db = Path(os.environ.get('NULLION_LIVE_RUNTIME_DB', str(Path.home() / '.nullion-stage' / 'runtime.db')))
    assert probe.main(['--base-url', base_url, '--runtime-db', str(runtime_db),
                       '--scenario', 'simple', '--scenario', 'weather', '--repeat', '2']) == 0
    prompt = 'Can u check top 10 news for today'
    conversation_id = f'web:live-stage-internal-browser-evidence:{uuid4().hex}'
    response, _ = probe._post_json(f'{base_url}/api/chat',
        {'text': prompt, 'conversation_id': conversation_id}, timeout=900)
    assert response.get('text'), response
    assert response.get('type') != 'approval_required', response
    assert response.get('artifacts') == [], response
    event = probe._chat_turn_for_user_message_with_poll(runtime_db, conversation_id, prompt)
    assert event and event.get('tool_results'), event
    results = event['tool_results']
    assert any(t['status'] == 'completed' and t['tool_name'] in
               {'web_search', 'web_fetch', 'browser_run_task', 'browser_navigate'} for t in results), results
    for tool in results:
        if tool.get('tool_name') == 'browser_run_task' and tool.get('status') == 'completed':
            output = tool['output']
            assert 'artifact_descriptors' in output, output
            assert all(d['role'] == 'source' for d in output['artifact_descriptors']), output

@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='requires real stage services')
def test_live_stage_targeted_scheduler_answer_and_core_tools(monkeypatch):
    import uuid
    import urllib.request
    from scripts import live_stage_chat_contract_probe as probe
    base_url = os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    runtime_db = Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage'/'runtime.db')))
    assert runtime_db == Path.home()/'.nullion-stage'/'runtime.db'
    name = 'Flight status verification ' + uuid.uuid4().hex[:8]
    created,_=probe._post_json(base_url+'/api/crons',{'name':name,'schedule':'*/30 * * * *',
        'task':'Monitor the configured condition.', 'enabled':False,
        'artifact_delivery_options':{'alert_delivery_mode':'verified_matches_only'}},timeout=30)
    job_id=created['job']['id']
    try:
        conversation_id = 'web:live-stage-targeted-scheduler:' + uuid.uuid4().hex
        prompt = f'Are you still tracking my flight ({name})? How often? Answer only about that tracker.'
        response, elapsed = probe._post_json(base_url+'/api/chat', {'text':prompt,'conversation_id':conversation_id}, timeout=180)
        event = probe._chat_turn_for_user_message_with_poll(runtime_db, conversation_id, prompt)
        assert 'list_crons' in probe._tool_names_from_chat_turn(event), response
        assert not {'toggle_cron','update_cron','delete_cron','run_cron','create_cron'} & probe._tool_names_from_chat_turn(event), event
        unchanged = probe._get_json(base_url+'/api/crons', timeout=30)
        rows = unchanged if isinstance(unchanged, list) else unchanged.get('jobs', unchanged.get('crons', []))
        current = next(row for row in rows if row['id']==job_id)
        assert current['enabled'] is False and current['schedule']==created['job']['schedule'], current
        text = response.get('text','').lower()
        assert '30' in text and any(word in text for word in ('paused','disabled','not active','not currently','off')), response
        assert 'here are your' not in text and 'email deadline' not in text, response
        print(json.dumps({'reply':response.get('text'), 'elapsed_ms':elapsed, 'tools':sorted(probe._tool_names_from_chat_turn(event))}))
        assert probe.main(['--base-url',base_url,'--runtime-db',str(runtime_db),
            '--scenario','simple','--scenario','weather','--repeat','2']) == 0
    finally:
        with urllib.request.urlopen(urllib.request.Request(base_url+'/api/crons/'+job_id,method='DELETE',headers=probe._live_request_headers({})),timeout=30) as response:
            assert response.status==200

@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='requires real stage services')
def test_live_stage_core_exec_is_discoverable_and_preserves_approval():
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    base_url=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    runtime_db=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage'/'runtime.db')))
    conversation_id='web:live-stage-core-exec:'+uuid4().hex
    prompt='Use core terminal execution to run printf nullion-core-exec-ok and return its output. This command only prints text.'
    response, elapsed=probe._post_json(base_url+'/api/chat',{'text':prompt,'conversation_id':conversation_id},timeout=120)
    event=probe._chat_turn_for_user_message_with_poll(runtime_db,conversation_id,prompt)
    names=probe._tool_names_from_chat_turn(event)
    assert 'terminal_exec' in names, response
    assert 'nullion-core-exec-ok' in str(response) or response.get('type')=='approval_required',response
    print(json.dumps({'reply':response.get('text'),'type':response.get('type'),'elapsed_ms':elapsed,'tools':sorted(names)}))


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1' or not os.environ.get('NULLION_LIVE_EMAIL_STATUS_PROMPT'),
    reason='requires real stage mailbox and an explicitly selected email-to-web status request')
def test_live_stage_email_to_web_answer_preserves_status_and_tool_evidence():
    """Opt-in replay uses a real mailbox and carrier pages, not an injected reply."""
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    base_url=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    runtime_db=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage'/'runtime.db')))
    prompt=os.environ['NULLION_LIVE_EMAIL_STATUS_PROMPT']
    conversation_id='web:live-stage-email-status:'+uuid4().hex
    response,elapsed=probe._post_json(base_url+'/api/chat',{'text':prompt,'conversation_id':conversation_id},timeout=240)
    print(json.dumps({'base_url':base_url,'conversation_id':conversation_id,'response':response},ensure_ascii=False))
    event=probe._chat_turn_for_user_message_with_poll(runtime_db,conversation_id,prompt)
    names=probe._tool_names_from_chat_turn(event)
    reply=response.get('text','')
    assert event is not None, response
    assert 'email_read' in names and any(name.startswith('browser_') for name in names),sorted(names)
    assert reply.strip() and 'Preview:' not in reply and '\nFrom:' not in reply,response
    assert reply==event['assistant_reply']
    print(json.dumps({'reply':reply,'elapsed_ms':elapsed,'tools':sorted(names)},ensure_ascii=False))
    assert probe.main(['--base-url',base_url,'--runtime-db',str(runtime_db),'--scenario','simple','--repeat','3'])==0


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Requires authorized live stage provider quota')
def test_live_stage_conceptual_answer_avoids_shell_completion_detour():
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    base_url = os.environ.get('NULLION_LIVE_BASE_URL', 'http://127.0.0.1:8753').rstrip('/')
    runtime_db = Path(os.environ.get('NULLION_LIVE_RUNTIME_DB', str(Path.home()/'.nullion-stage'/'runtime.db')))
    assert probe.main(['--base-url',base_url,'--runtime-db',str(runtime_db),
        '--scenario','simple','--scenario','weather','--repeat','2']) == 0
    conversation = f'web:privacy-answer:{uuid4().hex}'
    response, _ = probe._post_json(f'{base_url}/api/chat', {'conversation_id':conversation,
        'text':'What are some of the privacy concerns connecting Gemini with gmail?'}, timeout=180)
    text = response.get('text', '')
    assert len(text) > 300 and 'Local terminal capability verified' not in text, response
    turn = probe._chat_turn_for_user_message_with_poll(runtime_db, conversation,
        'What are some of the privacy concerns connecting Gemini with gmail?')
    assert turn is not None, conversation
    probe._latest_turn_latency_total_ms_with_poll(runtime_db, conversation, turn['turn_id'])
    rows = probe._conversation_collection_payloads(runtime_db, collection='conversation_events', conversation_id=conversation)
    assert turn['assistant_reply'] == text
    assert not any(row.get('tool_name') == 'terminal_exec' for row in turn.get('tool_results',[])), turn
    timings = [row for row in rows if row.get('event_type') == 'conversation.latency_timing']
    assert timings
    assert not any(phase.get('phase') == 'local_system_fallback_retry'
        for row in timings for phase in row.get('phase_timings',[])), timings


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='requires authorized live stage services')
def test_live_stage_independent_memory_and_history_recall_contract():
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    base_url=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    runtime_db=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage'/'runtime.db')))
    assert runtime_db.exists()
    assert probe.main(['--base-url',base_url,'--runtime-db',str(runtime_db),
        '--scenario','simple','--scenario','weather','--repeat','2'])==0
    token=uuid4().hex[:10]
    topic=f'Cedar-{token}'
    code=f'REF-{uuid4().hex[:10]}'
    seed=f'For our synthetic {topic} parcel, the reference is {code}. Earlier status was shipped. Acknowledge this only; no external lookup.'
    seed_conv=f'web:live-stage-recall-seed:{token}'
    seeded,_=probe._post_json(base_url+'/api/chat',{'text':seed,'conversation_id':seed_conv},timeout=180)
    assert seeded.get('type')!='approval_required',seeded
    seed_event=probe._chat_turn_for_user_message_with_poll(runtime_db,seed_conv,seed)
    assert seed_event,seeded
    archived,_=probe._post_json(base_url+f'/api/chat/history/{seed_conv}/archive',{},timeout=30)
    assert archived.get('ok'),archived
    prompt=f'What reference did we previously record for {topic}? Use saved history, and do not claim its present delivery status.'
    conv=f'web:live-stage-recall-independent:{token}'
    reply,elapsed=probe._post_json(base_url+'/api/chat',{'text':prompt,'conversation_id':conv},timeout=180)
    event=probe._chat_turn_for_user_message_with_poll(runtime_db,conv,prompt)
    assert code in reply.get('text',''),reply
    assert event and 'chat_history_search' in probe._tool_names_from_chat_turn(event),event
    assert not event.get('parent_turn_id'),event
    assert elapsed<90000,elapsed
    assert not {'email_search','email_read','calendar_list'} & set(probe._tool_names_from_chat_turn(event)),event
    assert not reply.get('artifacts'),reply


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='requires authorized live stage provider and tool execution')
def test_live_stage_reusable_tool_creation_and_independent_reuse_contract():
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    base_url=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    runtime_db=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage'/'runtime.db')))
    assert probe.main(['--base-url',base_url,'--runtime-db',str(runtime_db),
        '--scenario','simple','--scenario','weather','--repeat','2'])==0
    token=uuid4().hex[:10];name='stage_sum_'+token
    prompt=(f'Create a reusable Python tool named {name} in the persistent tool library. '
        'Search the library first, then use local_tool_save to save a function run(arguments) that sums '
        'arguments["values"] and returns {"total": sum}. Give it input and output JSON schemas. '
        'Use local_tool_run on that exact saved revision with values [12,18]. Tell me the result '
        'and that it is saved. Do not return source code or an attachment.')
    conversation='web:live-tool-create:'+token
    reply,elapsed=probe._post_json(base_url+'/api/chat',{'text':prompt,'conversation_id':conversation},timeout=240)
    event=probe._chat_turn_for_user_message_with_poll(runtime_db,conversation,prompt)
    assert event and reply.get('type')!='approval_required',(reply,event)
    results=event.get('tool_results',[])
    saved=next(r['output'] for r in results if r['tool_name']=='local_tool_save' and r['status']=='completed')
    ran=next(r['output'] for r in results if r['tool_name']=='local_tool_run' and r['status']=='completed')
    assert saved['revision']==ran['revision'] and ran['result']=={'total':30} and ran['verified'],results
    assert '30' in reply['text'] and not reply.get('artifacts'),reply
    assert elapsed<180000,elapsed
    prompt=f'Use the saved reusable tool {name} with values [41,59]. Tell me its result.'
    conversation='web:live-tool-reuse:'+token
    reply,elapsed=probe._post_json(base_url+'/api/chat',{'text':prompt,'conversation_id':conversation},timeout=180)
    event=probe._chat_turn_for_user_message_with_poll(runtime_db,conversation,prompt)
    assert event and reply.get('type')!='approval_required',(reply,event)
    assert 'local_tool_save' not in probe._tool_names_from_chat_turn(event),event
    ran=next(r['output'] for r in event['tool_results'] if r['tool_name']=='local_tool_run' and r['status']=='completed')
    assert ran['revision']==saved['revision'] and ran['result']=={'total':100},event
    assert '100' in reply['text'] and not reply.get('artifacts'),reply
    assert elapsed<120000,elapsed


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='requires authorized live stage services')
def test_live_stage_existing_source_path_remains_read_only_then_edits_explicitly():
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    db=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage'/'runtime.db')))
    root=Path(os.environ.get('NULLION_LIVE_WORKSPACE_ROOT',str(Path.home()/'.nullion-stage'/'workspaces'/'workspace_admin')))
    note=root/'files'/('source-role-'+uuid4().hex[:8]+'.txt')
    note.parent.mkdir(parents=True,exist_ok=True);note.write_text('Recorded value: 17\n',encoding='utf-8')
    conversation='web:source-role:'+uuid4().hex
    virtual='/workspace/files/'+note.name
    try:
        prompt=f'Read {virtual} and tell me the recorded value. Leave the note unchanged.'
        response,elapsed=probe._post_json(base+'/api/chat',{'text':prompt,'conversation_id':conversation},timeout=180)
        event=probe._chat_turn_for_user_message_with_poll(db,conversation,prompt)
        assert note.read_text()=='Recorded value: 17\n'
        assert '17' in response.get('text','') and not response.get('artifacts'),response
        names=probe._tool_names_from_chat_turn(event)
        assert 'file_read' in names and not names.intersection({'file_write','file_patch','local_tool_save'}),event
        edit=f'Change the recorded value in {virtual} to 23 and confirm the value here. Keep it as a note, without sending an attachment.'
        changed,edit_elapsed=probe._post_json(base+'/api/chat',{'text':edit,'conversation_id':conversation},timeout=180)
        changed_event=probe._chat_turn_for_user_message_with_poll(db,conversation,edit)
        assert '23' in note.read_text() and '17' not in note.read_text(),changed
        assert '23' in changed.get('text','') and not changed.get('artifacts'),changed
        assert probe._tool_names_from_chat_turn(changed_event).intersection({'file_patch','file_write'}),changed_event
        print(json.dumps({'read_reply':response['text'],'edit_reply':changed['text'],
            'read_ms':elapsed,'edit_ms':edit_elapsed,'read_tools':sorted(names)},ensure_ascii=False))
    finally:
        note.unlink(missing_ok=True)


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or not os.environ.get('NULLION_LIVE_SOURCE_COMPARISON_PROMPT'),
    reason='requires authorized live source comparison and expected quoted values')
def test_live_stage_source_comparison_retains_verified_quotes():
    """Supporting live API evidence; Web/Telegram UI replay remains separate."""
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    base_url = os.environ.get('NULLION_LIVE_BASE_URL', 'http://127.0.0.1:8753').rstrip('/')
    runtime_db = Path(os.environ.get('NULLION_LIVE_RUNTIME_DB', str(Path.home()/'.nullion-stage'/'runtime.db')))
    prompt = os.environ['NULLION_LIVE_SOURCE_COMPARISON_PROMPT']
    expected = json.loads(os.environ['NULLION_LIVE_SOURCE_EXPECTED_VALUES'])
    assert isinstance(expected, list) and len(expected) >= 2
    conversation = 'web:live-source-comparison:'+uuid4().hex
    response, elapsed = probe._post_json(base_url+'/api/chat',
        {'text': prompt, 'conversation_id': conversation}, timeout=600)
    event = probe._chat_turn_for_user_message_with_poll(runtime_db, conversation, prompt)
    assert event and event['assistant_reply'] == response['text'], response
    results = event.get('tool_results', [])
    evidence = json.dumps([r['output'] for r in results if r.get('status') == 'completed'], ensure_ascii=False)
    assert all(value in evidence and value in response['text'] for value in expected), response
    assert any(r.get('tool_name', '').startswith('browser_') for r in results), results
    assert "I couldn't verify all requested details" not in response['text'], response
    if os.environ.get('NULLION_LIVE_SOURCE_EXPECTED_BROWSER_CALLS'):
        browser_runs = [r for r in results if r.get('tool_name') == 'browser_run_task']
        assert len(browser_runs) == int(os.environ['NULLION_LIVE_SOURCE_EXPECTED_BROWSER_CALLS']), results
        assert all(r['status'] == 'completed' and r['output'].get('observations') for r in browser_runs), results
    from urllib.parse import quote
    history = probe._get_json(base_url + '/api/chat/history/' + conversation +
        '?turn_ids=' + quote(json.dumps([event['turn_id']])), timeout=30)
    assert history['terminal_replies'][0]['text'] == response['text'], history
    assert not response.get('artifacts'), response
    print(json.dumps({'conversation_id': conversation, 'elapsed_ms': elapsed,
        'reply': response['text'], 'tools': sorted(probe._tool_names_from_chat_turn(event))}, ensure_ascii=False))
    assert probe.main(['--base-url', base_url, '--runtime-db', str(runtime_db),
        '--scenario', 'simple', '--scenario', 'weather', '--repeat', '2']) == 0


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='requires authorized live stage services')
def test_live_stage_draft_artifact_is_never_delivered_or_restored():
    """Real provider, API, persistence, history, and downloaded final bytes."""
    import io
    from uuid import uuid4
    from docx import Document
    from scripts import live_stage_chat_contract_probe as probe
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    db=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage'/'runtime.db')))
    token=uuid4().hex[:10]
    draft=f'stage_draft_{token}.docx';final=f'stage_final_{token}.docx'
    prompt=(f'Use document_create to create a temporary draft named {draft} with the synthetic sentence '
            f'"Juniper inventory draft: 10 units." Then use document_create to create {final} with '
            '"Juniper inventory final: 12 units." Deliver only the final DOCX. The draft is an intermediate '
            'working file and must not be attached. Inspect the final file before delivering it.')
    conversation='web:live-artifact-revision:'+token
    reply,elapsed=probe._post_json(base+'/api/chat',{'text':prompt,'conversation_id':conversation},timeout=240)
    event=probe._chat_turn_for_user_message_with_poll(db,conversation,prompt)
    assert event,reply
    produced=[r for r in event.get('tool_results',[]) if r.get('tool_name')=='document_create' and r.get('status')=='completed']
    assert len(produced)==2,event
    delivered=reply.get('artifacts',[])
    assert len(delivered)==1 and Path(delivered[0]['path']).name==final,reply
    assert [Path(p).name for p in event.get('artifact_paths',[])]==[final],event
    import urllib.parse
    query=urllib.parse.urlencode({'turn_ids':json.dumps([event['turn_id']])})
    history=probe._get_json(base+'/api/chat/history/'+conversation+'?'+query,timeout=30)
    replies=history.get('terminal_replies',[])
    assert len(replies)==1,history
    assert [Path(a['path']).name for a in replies[0].get('artifacts',[])]==[final],history
    import urllib.request
    with urllib.request.urlopen(base+delivered[0]['url'],timeout=30) as response:
        data=response.read()
    text='\n'.join(p.text for p in Document(io.BytesIO(data)).paragraphs)
    assert '12 units' in text and '10 units' not in text,text
    assert elapsed<180000,elapsed
    print(json.dumps({'conversation_id':conversation,'elapsed_ms':elapsed,'final':final,'draft':draft,'artifact_count':len(delivered)},ensure_ascii=False))


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='real stage model and browser quota')
def test_live_stage_research_discloses_missing_information_once(record_property):
    from uuid import uuid4
    from urllib.parse import quote
    from scripts import live_stage_chat_contract_probe as probe
    base = os.environ.get('NULLION_LIVE_BASE_URL', 'http://127.0.0.1:8753').rstrip('/')
    db = Path(os.environ.get('NULLION_LIVE_RUNTIME_DB', str(Path.home() / '.nullion-stage/runtime.db')))
    prompt = ('Read https://example.com and give me its main heading and the exact number of visitors '
        'it had yesterday. Check the site itself. Return the findings and clearly identify anything '
        'the source does not establish. Do not create or attach files.')
    conversation = 'web:live-source-disclosure:' + uuid4().hex[:12]
    response, elapsed = probe._post_json(base + '/api/chat', {'text': prompt,
        'conversation_id': conversation}, timeout=300)
    event = probe._chat_turn_for_user_message_with_poll(db, conversation, prompt)
    assert event, response
    reports = [r for r in event.get('tool_results', [])
        if r.get('output', {}).get('result_kind') == 'source_observations']
    tools = probe._tool_names_from_chat_turn(event)
    assert any(name in tools for name in ('browser_extract_text', 'browser_run_task')), event
    text = response.get('text', '')
    assert 'Example Domain' in text and not response.get('artifacts'), response
    if any(r['output'].get('report_status') == 'partial' for r in reports):
        assert text.count('Not verified from this check:') == 1, response
    else:
        # Direct extraction need not invoke the optional source presenter.
        # It must still explain the absent figure once, with runtime evidence.
        assert text.lower().count('visitors yesterday') == 1, response
        assert text.lower().count('not established') == 1, response
    assert "I couldn't complete this request because" not in text, response
    assert event['assistant_reply'] == text
    history = probe._get_json(base + '/api/chat/history/' + conversation +
        '?turn_ids=' + quote(json.dumps([event['turn_id']])), timeout=30)
    assert history['terminal_replies'][0]['text'] == text
    record_property('elapsed_ms', elapsed)
    print(json.dumps({'conversation_id': conversation, 'elapsed_ms': elapsed,
        'tool_names': sorted(tools), 'text': text}, ensure_ascii=False))

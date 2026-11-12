"""Real stage Web-composer acceptance. External providers are never mocked.

Run only in Docker with stage mounted read-only for backing runtime evidence.
Requests are submitted with browser input; API helpers only inspect evidence
and download final artifact bytes. No real scheduled task is toggled or run.
"""
from __future__ import annotations

from dataclasses import replace
import importlib.util
import hashlib
import json
import os
import re
from pathlib import Path
import sqlite3
import sys
import time
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_LIVE_STAGE_E2E") != "1",
    reason="Explicit live stage acceptance only",
)

APP = Path(os.environ.get("NULLION_APP_REPO", "/tmp/nullion-src"))
spec = importlib.util.spec_from_file_location(
    "stage_web_contract_catalogue", APP / "scripts/live_stage_chat_contract_probe.py"
)
catalogue = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = catalogue
spec.loader.exec_module(catalogue)

# Explicitly selected read-only and generated-deliverable scenarios. Sending
# email, bulk enabling schedules, and arbitrary real cron runs are excluded.
CASES = [
    "simple", "startup_warm_first_chat", "plain_chat_attachment_planner_fast_path",
    "local_system_terminal_fallback", "local_system_followup_terminal_fallback",
    "local_system_update_followup_terminal_outcome", "scheduler_inventory_read_only",
    "single_numbered_choice_list_contract", "vague_account_source_selection",
    "weather", "profile_weather", "calendar", "calendar_tomorrow_live_access",
    "calendar_weekend_live_access", "calendar_answer_not_raw_listing",
    "email_simple_live_access", "email_urgent_read_recovery", "email_read_followup",
    "email_request_overrides_pr_context", "email_request_overrides_security_history",
    "github_prs", "browser_price_stock_no_files", "browser_not_found_catalog_guard",
    "browser_flight_live_records", "browser_rental_live_records", "market_quote_live_records",
    "browser_continuation_registry_guard", "browser_followup_account_scope_guard",
    "browser_page_screenshot_artifact_delivery", "svg_artifact_file_write",
    "artifact_preflight_scope_fast_path", "artifact_twenty_row_xlsx_delivery",
    "chat_history_memory_recall_artifact", "recent_context_artifact_reuse_chain",
    "artifact_context_followup", "html_research_real_images",
    "mixed_embedded_media_artifact_package", "random_website_artifact_bundle",
    "formatted_pdf_flyer_pack", "spreadsheet_visual_workbook_pack",
    "kids_playbook_artifact_pack", "executive_personal_briefing_pack",
    "complex_artifact_pack", "image", "browser_filter_followup",
    "archive_download_extract_manifest", "archive_tool_fallback_followup_chain", "ambiguous_report_reference",
]


def _turns(database, conversation):
    connection=os.environ.get("NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION")
    if connection:
        from urllib.request import Request,urlopen
        from urllib.parse import urlencode
        settings=json.loads(Path(connection).read_text())
        request=Request("http://host.docker.internal:"+str(settings['port'])+"/turns?"+
            urlencode({'conversation_id':conversation}),headers={'Authorization':'Bearer '+settings['token']})
        from scripts.live_stage_http import read_json
        payload=read_json(request,timeout=15,opener=urlopen)
        assert payload['conversation_id']==conversation
        return payload['turns']
    # Use the indexed conversation key. Read-only evidence cannot alter stage.
    deadline = time.monotonic() + 10
    while True:
        try:
            with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=10) as db:
                return [json.loads(row[0]) for row in db.execute(
                    "SELECT payload FROM conversation_events "
                    "WHERE json_extract(payload,'$.conversation_id')=? "
                    "AND json_extract(payload,'$.event_type')='conversation.chat_turn' "
                    "ORDER BY updated_at", (conversation,))]
        except sqlite3.DatabaseError:
            # Docker Desktop's shared-file view can catch a host checkpoint
            # mid-write. Retry only the read, never resubmit the user turn.
            if time.monotonic() >= deadline:
                raise
            time.sleep(.2)


def watch_chat_frames(page, frames, errors, base, *, on_frame=None):
    """Attach both supported transports whenever a probe opens a chat page."""
    def accept(data):
        if isinstance(data,dict):
            frames.append(data)
            if on_frame is not None:on_frame(data)
    def received(raw):
        try:accept(json.loads(raw))
        except (ValueError,TypeError):pass
    def response(item):
        if item.request.method=='POST' and item.url==base+'/api/chat':
            data=item.json()
            accept({**data,'type':'done' if data.get('type')=='message' else data.get('type'),
                'transport':'web-http-fallback'})
    page.on('pageerror',lambda error:errors.append(str(error)))
    page.on('websocket',lambda socket:socket.on('framereceived',received))
    page.on('response',response)


@pytest.fixture
def live_web(request):
    from playwright.sync_api import sync_playwright
    assert os.environ.get("NULLION_E2E_CONTAINER") == "1", "Docker required"
    base = os.environ.get("NULLION_LIVE_BASE_URL", "http://127.0.0.1:8753")
    assert base == "http://127.0.0.1:8753", "Stage-only loopback forwarder required"
    readiness_deadline = time.monotonic() + 20
    while True:
        try:
            assert catalogue._get_json(base + "/api/health", timeout=2)["status"] == "ok"
            break
        except Exception:
            if time.monotonic() >= readiness_deadline:
                raise
            time.sleep(.2)
    database = Path(os.environ.get("NULLION_LIVE_RUNTIME_DB", "/stage/runtime.db"))
    conversation = "web:live-ui-audit:" + uuid4().hex
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');"
                                "localStorage.setItem('nullion_conv_id'," + json.dumps(conversation) + ");")
        page = context.new_page()
        frames, errors = [], []
        live_evidence = Path(os.environ.get("NULLION_LIVE_EVIDENCE_DIR", "/tmp/live-web-evidence")) / "active-frames"
        live_evidence.mkdir(parents=True, exist_ok=True, mode=0o700)
        def save_live_frame(data):
            with (live_evidence / (conversation.replace(":", "_") + ".jsonl")).open("a") as stream:
                stream.write(json.dumps({"received_at": time.time(), "frame": data}, ensure_ascii=False) + "\n")
        watch_chat_frames(page,frames,errors,base,on_frame=save_live_frame)
        page.goto(base, wait_until="domcontentloaded")
        page.locator("#user-input").wait_for(state="visible")
        try:
            yield page, frames, errors, database, conversation, base
        finally:
            evidence = Path(os.environ.get("NULLION_LIVE_EVIDENCE_DIR", "/tmp/live-web-evidence")) / "ui-boundary" / (request.node.name.split("[", 1)[0][:100] + "-" + hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:16])
            evidence.mkdir(parents=True, exist_ok=True, mode=0o700)
            screenshot_error = None
            try:
                page.screenshot(path=str(evidence / "web.png"), full_page=False, timeout=10000)
            except Exception as error:
                screenshot_error = str(error)
            (evidence / "frames.json").write_text(json.dumps({"conversation_id":conversation,
                "frames":frames,"page_errors":errors,"screenshot_error":screenshot_error}, ensure_ascii=False, indent=2))
            try:
                saved = getattr(request.node, '_nullion_saved_background_execution', None)
                if isinstance(saved, dict) and saved.get('terminal') is False:
                    # A broken renderer/observation window is not user intent
                    # to cancel an accepted background task. Preserve the owned
                    # run for diagnosis instead of changing its terminal cause.
                    cleanup = {'stopped': False, 'reason': 'saved_execution_kept_for_diagnosis',
                               'cron_id': saved['cron_id'], 'conversation_id': conversation}
                    (evidence/'cleanup.json').write_text(json.dumps(cleanup,ensure_ascii=False,indent=2))
                else:
                    stopped=page.request.post(base+'/api/chat',data={'text':'/stop','conversation_id':conversation},timeout=30000)
                    assert stopped.ok, stopped.text()
                    (evidence/'cleanup.json').write_text(json.dumps(stopped.json(),ensure_ascii=False,indent=2))
            finally:
                browser.close()


def _submit(live_web, prompt, timeout=240, poll_full_status=False, audit_prompt=None):
    from playwright.sync_api import expect
    from scripts.live_stage_provider_guard import process_provider_guard
    provider_guard = process_provider_guard()
    page, frames, errors, database, conversation, base = live_web
    before = len(frames)
    prior_turn_ids = {row.get('turn_id') for row in _turns(database, conversation)}
    started = time.monotonic()
    page.locator("#user-input").fill(prompt)
    page.locator("#user-input").press("Enter")
    terminal = None
    next_status_poll = started + 1
    next_provider_check = started
    while time.monotonic() - started < timeout:
        if time.monotonic() >= next_provider_check or provider_guard.check_pending:
            blocked = provider_guard.poll()
            if blocked:
                pytest.exit(blocked, returncode=1)
            next_provider_check = time.monotonic() + 15
        candidates = [frame for frame in frames[before:]
                      if frame.get("type") in {"done", "error", "approval_required"}
                      and frame.get("conversation_id", conversation) == conversation
                      and not frame.get("superseded")]
        if candidates:
            terminal = candidates[-1]
            break
        if poll_full_status and time.monotonic() >= next_status_poll:
            status = page.request.get(base + '/api/status?full=1', timeout=15000)
            assert status.ok, status.text()
            next_status_poll = time.monotonic() + 2
        page.wait_for_timeout(100)
    if terminal is None:
        # Cancel only this test-owned conversation before opening another
        # shared-browser workflow. A closed browser tab is not cancellation.
        page.locator("#user-input").fill("/stop")
        page.locator("#user-input").press("Enter")
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if any(frame.get("type") == "session_stopped" for frame in frames[before:]):
                break
            page.wait_for_timeout(100)
        raise AssertionError(f"No visible terminal response after {timeout}s; requested scoped cancellation")
    assert terminal["type"] != "error", terminal
    assert terminal["type"] != "approval_required", (
        "Unexpected permission prompt for a read-only or task-owned artifact request", terminal)
    terminal_elapsed_ms = (time.monotonic() - started) * 1000
    turn = None
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        turn = catalogue._select_new_chat_turn(_turns(database, conversation), audit_prompt if audit_prompt is not None else prompt,
            turn_id=terminal.get('turn_id'), prior_turn_ids=prior_turn_ids)
        if turn:
            break
        page.wait_for_timeout(200)
    if turn is None:
        # Plain turns deliberately defer the large runtime checkpoint. Their
        # canonical visible history is still synchronously persisted.
        history = catalogue._get_json(base + "/api/chat/history/" + conversation, timeout=10)
        candidates = [message for message in history["messages"]
                      if message.get("role") == "bot"
                      and ((terminal.get("turn_id") and message.get("metadata", {}).get("runtime_turn_id") == terminal["turn_id"])
                           or (not terminal.get("turn_id") and message.get("text") == terminal.get("text")))]
        assert candidates, "Web terminal has no durable chat-history receipt"
        receipt = candidates[-1]
        turn = {"turn_id": receipt.get("metadata", {}).get("runtime_turn_id"), "assistant_reply": receipt["text"],
                "user_message": prompt, "tool_results": [], "evidence_kind": "deferred_runtime_checkpoint"}
    # The delivery boundary applies saved presentation preferences (for
    # example omitting emoji) after the agent's runtime turn is recorded.
    # reply_already_sent completes with an empty done.text; its chunk frames
    # are the actual delivered copy, which can include the saved activity view.
    # Durable history may retain a differently presented canonical receipt.
    delivered_chunks = "".join(str(frame.get("text") or "") for frame in frames[before:]
        if frame.get("type") == "chunk" and frame.get("conversation_id", conversation) == conversation and (not terminal.get("turn_id")
            or frame.get("turn_id") == terminal.get("turn_id")))
    reply = (terminal.get("text") or delivered_chunks or turn.get("assistant_reply", "")).strip()
    turn["delivered_reply"] = reply
    assert reply or terminal.get("artifacts"), "Empty terminal response"
    turn_id = terminal.get("turn_id") or turn.get("turn_id")
    bubble = page.locator('.msg.bot .bubble[data-turn-id=' + json.dumps(turn_id) + ']')
    expect(bubble).to_have_count(1, timeout=10000)
    expect(bubble).to_be_visible()
    raw_text = bubble.get_attribute("data-raw-text").strip()
    # Activity display may append its separate status section to raw text.
    # The complete final reply must appear once, at the beginning.
    if terminal.get("artifacts"):
        # Artifact receipts may replace sandbox links with a download caption
        # after the streamed text was rendered. Verify the actual attachment
        # cards and bytes below; require a nonempty visible terminal here.
        assert raw_text and bubble.inner_text().strip(), (raw_text, reply)
    else:
        # The wire may include typed activity content that the UI renders in
        # a separate card. Compare the actual message with its durable receipt,
        # without parsing or stripping English activity labels from the wire.
        history = catalogue._get_json(base + "/api/chat/history/" + conversation, timeout=15)
        stored = [row for row in history.get("messages", []) if row.get("role") == "bot"
            and row.get("metadata", {}).get("runtime_turn_id") == turn_id]
        assert stored and raw_text == stored[-1]["text"].strip(), (
            "Rendered reply differs from its stored message", bubble.inner_text(), stored)
        assert bubble.inner_text().strip(), "Stored reply has no visible message"
        reply = raw_text
        turn["delivered_reply"] = reply
    assert not errors, errors
    response = {"text": reply, "artifacts": terminal.get("artifacts", [])}
    return response, turn, terminal_elapsed_ms


@pytest.mark.parametrize("name", CASES)
def test_live_stage_web_catalogue(name, live_web):
    page, frames, errors, database, conversation, base = live_web
    scenario = catalogue.SCENARIOS[name]
    if ('image_generate' in scenario.required_tool_all or scenario.required_tool_any == ('image_generate',)):
        config=catalogue._get_json(base+'/api/config',timeout=15)
        if not config.get('image_generate_enabled') or not config.get('image_generate_available'):
            pytest.skip('Stage image_generate is disabled or unavailable; media configuration is unchanged')
    if name == "browser_flight_live_records":
        # The original request omits trip type/passenger count. Exercise the
        # numeric clarification and then require a completed lookup.
        scenario = replace(scenario, followups=(
            "1. One adult, economy, one-way JFK to ORD tomorrow. Any departure time. "
            "Show up to three bookable options with their displayed total USD fares; do not book.",))
    if name == "browser_rental_live_records":
        # Fixed June dates become ambiguous after the original incident. Keep
        # the real rental boundary, with explicit future dates and times.
        from datetime import date, timedelta
        pickup=date.today()+timedelta(days=14)
        returned=pickup+timedelta(days=6)
        scenario=replace(scenario,prompt=(
            f"Find an Enterprise rental car in downtown Providence, Rhode Island, USA, pickup "
            f"{pickup.isoformat()} at 12:30 PM and return {returned.isoformat()} at 12:30 PM. "
            "One driver aged 39; compare economy or compact cars. Give actual available options "
            "and displayed total USD prices, or identify the specific live-search blocker. "
            "Read-only: do not book, sign in, or submit personal information."))
    if name == "calendar":
        # A broad calendar request may legitimately ask for a date window.
        # Complete that conversation and require actual account evidence.
        scenario = replace(scenario, followups=("Check today only.",))
    if name == "email_request_overrides_security_history":
        # Seed URL context without overwriting a workspace-wide memory key.
        scenario = replace(scenario, prompt=(
            "For this conversation only, https://myaccount.google.com/notifications "
            "is a reference page about Google security notifications. Acknowledge only; "
            "do not check email or save durable memory for this setup turn."))
    if name == "email_urgent_read_recovery":
        # A preview-only urgency answer can be valid. Explicitly request body
        # reads as well so this replay reaches the optional-link regression.
        scenario = replace(scenario, followups=(
            "Read the message bodies from that inbox search and reassess whether anything needs action. "
            "Do not open links or attachments merely because they exist. No files.",))
    if name == "email_read_followup":
        # A body summary need not literally label itself "summary" or repeat
        # mail headers. Require the actual detail read and substantive text.
        scenario = replace(scenario, required_text_any=(),
            required_tool_all=tuple(dict.fromkeys((*scenario.required_tool_all, "email_read"))))
    assert scenario.runner == "http" and not scenario.requires_email_send_flag
    output = Path(os.environ.get("NULLION_LIVE_EVIDENCE_DIR", "/tmp/live-web-evidence")) / name
    output.mkdir(parents=True, exist_ok=True)
    responses, turns, samples = [], [], []
    proof = {"scenario": name, "conversation_id": conversation, "transport": "web-composer"}
    (output / "result.json").write_text(json.dumps({**proof, "status": "running"}, indent=2))
    try:
        for prompt in (scenario.prompt, *scenario.followups):
            # Keep a long external browser run observable through its terminal
            # report; the contract below still enforces the shorter latency
            # target. Closing its client early would discard the diagnosis.
            timeout = 720 if name in {"browser_rental_live_records", "browser_flight_live_records", "browser_filter_followup", "html_research_real_images", "random_website_artifact_bundle", "complex_artifact_pack", "kids_playbook_artifact_pack", "executive_personal_briefing_pack"} else max(
                90, (scenario.max_single_ms or 180000) / 1000 + 30)
            response, turn, elapsed = _submit(live_web, catalogue._materialized_prompt(
                prompt, scenario_name=name, index=1), timeout=timeout)
            catalogue._assert_turn_tools(scenario, turn)
            responses.append(response); turns.append(turn); samples.append(elapsed)
            if name in {'chat_history_memory_recall_artifact', 'recent_context_artifact_reuse_chain', 'artifact_context_followup'} and response.get('artifacts'):
                # Do not spend more provider calls transforming an unfulfilled
                # intermediate artifact. Keep its failed request evidence here.
                assert turn.get('response_fulfilled') is True,turn
            assert "could not turn it into a focused answer" not in response["text"], response
        if name == 'email_request_overrides_pr_context':
            latest=turns[-1]
            assert not any(row['tool_name'].startswith('browser_') for row in latest.get('tool_results',[])), (
                'The new account request continued the previous website task',latest)
            assert not any(value.casefold() in responses[-1]['text'].casefold() for value in scenario.forbidden_text),responses[-1]
            completed={row['tool_name'] for row in latest.get('tool_results',[]) if row.get('status')=='completed'}
            if not completed.intersection({'email_search','email_read'}):
                # A broad account read may ask for a window. Finish that human
                # exchange, while still requiring the actual account evidence.
                response,turn,elapsed=_submit(live_web,
                    "Check today's inbox and unread messages. Summarize anything needing action. No files.",timeout=150)
                responses.append(response);turns.append(turn);samples.append(elapsed)
                assert any(row['tool_name'] in {'email_search','email_read'} and row['status']=='completed'
                    for row in turn.get('tool_results',[])),turn
        if name in {"calendar_tomorrow_live_access", "email_request_overrides_security_history"}:
            read_names = {"calendar_list"} if name == "calendar_tomorrow_live_access" else {"email_search", "email_read"}
            completed = {result["tool_name"] for item in turns for result in item.get("tool_results", [])
                         if result.get("status") == "completed"}
            if not completed.intersection(read_names):
                # A genuine account choice is a valid intermediate response.
                # Select its first visible numbered option, then require the
                # actual account read. No language-specific intent inference.
                options = re.findall(r"(?m)^\s*(\d+)[.)]\s+", responses[-1]["text"])
                assert len(options) >= 2 and options[0] == "1", responses[-1]
                response, turn, elapsed = _submit(live_web, "1", timeout=max(90, (scenario.max_single_ms or 180000) / 1000 + 30))
                responses.append(response); turns.append(turn); samples.append(elapsed)
        if name in {"calendar", "calendar_tomorrow_live_access", "calendar_weekend_live_access", "calendar_answer_not_raw_listing"}:
            reviews=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.completion_review')
            latest=reviews[-1] if reviews else {}
            if (latest.get('disposition')=='needs_user_input'
                    and latest.get('source_consent',{}).get('source_selection_required') is True):
                # An independent date-only request can need a real source choice.
                # Complete the numbered UI exchange, then demand a final read.
                choices=re.findall(r'(?m)^\s*(\d+)[.)]\s+', responses[-1]['text'])
                assert len(choices)>=2 and choices[0]=='1', responses[-1]
                response,turn,elapsed=_submit(live_web,'1',timeout=90)
                responses.append(response);turns.append(turn);samples.append(elapsed)
                assert any(r['tool_name']=='calendar_list' and r['status']=='completed'
                    for r in turn.get('tool_results',[])), 'Selected calendar was not read'
                reviews=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.completion_review')
            assert reviews and reviews[-1]['disposition']=='complete', 'Calendar lookup did not reach verified completion'
        response = dict(responses[-1])
        if name == "email_read_followup":
            assert len(response["text"]) >= 120, response
        response["artifacts"] = [artifact for item in responses for artifact in item["artifacts"]]
        if (scenario.required_tool_all or scenario.required_tool_any) and any(
                turn.get('evidence_kind')=='deferred_runtime_checkpoint' for turn in turns):
            # Visible delivery is synchronous; its secondary audit checkpoint may
            # finish later. Keep original UI latency and separately require the
            # actual durable tool receipt, rather than inventing an empty audit.
            audit_deadline=time.monotonic()+60
            pending={turn['turn_id'] for turn in turns if turn.get('evidence_kind')=='deferred_runtime_checkpoint'}
            while pending and time.monotonic()<audit_deadline:
                durable={turn.get('turn_id'):turn for turn in _turns(database,conversation)}
                for index,turn in enumerate(turns):
                    saved=durable.get(turn.get('turn_id'))
                    if turn.get('turn_id') in pending and saved:
                        turns[index]={**saved,'delivered_reply':turn.get('delivered_reply',saved.get('assistant_reply'))}
                        pending.remove(turn['turn_id'])
                if pending:page.wait_for_timeout(250)
            assert not pending,('The visible answer has no durable runtime audit after 60 seconds',pending)
        results = [result for turn in turns for result in turn.get("tool_results", [])]
        if name in {'browser_rental_live_records','browser_flight_live_records'}:
            # Words such as price/total do not prove a completed lookup. Both
            # verified options and an explicitly permitted, observed blocker
            # must pass the request-owned material outcome review.
            assert turns[-1].get('response_fulfilled') is True,turns[-1]
            reviews=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.completion_review')
            assert reviews and reviews[-1].get('disposition')=='complete',reviews[-1:]
            proof['lookup_request_completed']=True
        if name == "scheduler_inventory_read_only":
            inventories = [row["output"] for row in results if row["tool_name"] == "list_crons"
                           and row["status"] == "completed"]
            assert inventories
            index_rows={}
            for inventory in inventories:
                if inventory.get("inventory_index_complete"):
                    index_rows.update({row['selection_index']:row for row in inventory['inventory_index']})
            if index_rows:
                from tests.e2e.live_inventory_assertions import assert_numbered_inventory_identities
                proof['inventory_identities']=assert_numbered_inventory_identities(response['text'],
                    {'inventory_index':list(index_rows.values())})
        names = {result["tool_name"] for result in results}
        if name == "email_urgent_read_recovery":
            completed_account = {result["tool_name"] for result in results
                                 if result.get("status") == "completed"}
            assert {"email_search", "email_read"}.issubset(completed_account), results
            assert not names.intersection({"browser_navigate", "browser_extract_text", "web_fetch"}), (
                "Optional email links restarted an already completed account read", results)
            assert "inbox search did not run" not in response["text"].casefold(), response
        explicit_worker_selected = False
        for result in results:
            if result.get("tool_name") != "request_tool_scope" or result.get("status") != "completed":
                continue
            scope = result.get("output") or {}
            selected = set(scope.get("explicit_tool_names") or ()).union(scope.get("requested_required_tool_names") or ())
            explicit_worker_selected |= "browser_run_task" in selected
            if not explicit_worker_selected:
                assert "browser_run_task" not in scope.get("available_tools", ()), scope
        contract = catalogue._scenario_for_config(scenario, catalogue._get_json(base + "/api/config", timeout=15))
        if name == "recent_context_artifact_reuse_chain":
            # The request names Summary, but does not prescribe the data
            # worksheet's title. Check actual values instead of inventing one.
            contract = replace(contract, required_xlsx_sheet_names=("Summary",))
            import io
            from openpyxl import load_workbook
            from pypdf import PdfReader
            for artifact in response["artifacts"]:
                extension = Path(catalogue._artifact_name(artifact)).suffix.lower()
                if extension not in {".xlsx", ".pdf"}:
                    continue
                body, _ = catalogue._artifact_bytes(base, artifact, timeout=30)
                if extension == ".xlsx":
                    workbook = load_workbook(io.BytesIO(body), data_only=False)
                    actual = {tuple(row[:2]) for sheet in workbook if sheet.title != "Summary" for row in sheet.values}
                    assert {("North",12),("South",19),("East",7),("West",15)}.issubset(actual),actual
                    totals = [cell for row in workbook["Summary"].values for cell in row]
                    assert 53 in totals or any(isinstance(cell,str) and cell.upper().startswith("=SUM(") for cell in totals),totals
                else:
                    text = " ".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(body)).pages)
                    assert "53" in text and "South" in text,text
        browser_reports=[result["output"] for result in results
            if result["tool_name"]=="browser_run_task" and result["status"]=="completed"
            and isinstance(result.get("output"),dict)]
        if browser_reports and set(contract.required_tool_any).intersection({
                "browser_navigate","browser_extract_text","browser_extract_items","web_fetch"}):
            # The real delegated browser is an equivalent source boundary,
            # provided its typed, audited observations actually exist.
            assert all(report.get("result_kind")=="source_observations"
                and report.get("execution_audit",{}).get("complete") is True
                and (report.get("observations") or report.get("blocker"))
                for report in browser_reports),browser_reports
            contract=replace(contract,required_tool_any=tuple(dict.fromkeys(
                (*contract.required_tool_any,"browser_run_task"))))
        if contract.require_loaded_browser_capture:
            isolated = [result["output"] for result in results if result["tool_name"]=="browser_run_task"
                        and result["status"]=="completed"]
            if isolated:
                assert all(result.get("goal_status")=="answered" and not result.get("blocker")
                    and result.get("execution_audit",{}).get("complete") is True
                    and result.get("page_url") in result.get("execution_audit",{}).get("visited_urls",[])
                    and any(item.get("role")=="deliverable" and item.get("extension")==".png"
                            for item in result.get("artifact_descriptors",[])) for result in isolated), isolated
                contract = replace(contract, require_loaded_browser_capture=False,
                    required_tool_any=tuple(dict.fromkeys((*contract.required_tool_any,"browser_run_task"))))
        assert "could not turn it into a focused answer" not in response["text"], response
        if name == "image":
            generated = [row.get('output', {}) for turn in turns for row in turn.get('tool_results', [])
                if row.get('tool_name') == 'image_generate' and row.get('status') == 'completed']
            assert generated and any(not row.get('fallback_used') for row in generated), generated
        if name == "html_research_real_images":
            # The request requires itineraries, not an exact English heading.
            # Their actual ordered steps are verified in the downloaded HTML.
            contract=replace(contract,required_artifact_text_all=())
        if name == "spreadsheet_visual_workbook_pack":
            # The prompt permits generated visual markers as an alternative
            # to raster images. Verify those plus every requested formula.
            contract=replace(contract,required_embedded_media_extensions=())
        catalogue._assert_contract(contract, response=response, elapsed_ms=max(samples),
            tool_names=names, runtime_db_available=True,
            tool_output_texts=[json.dumps(result) for result in results],
            latency_advisories=proof.setdefault('latency_advisories',[]),check_latency=False)
        if name in {"browser_price_stock_no_files", "browser_continuation_registry_guard",
                    "browser_followup_account_scope_guard"}:
            reviews = catalogue._conversation_event_payloads(database, conversation, event_type="conversation.completion_review")
            if name=="browser_followup_account_scope_guard":
                from tests.e2e.live_public_followup_assertions import assert_public_followup_outcome
                proof['public_followup_boundary']=assert_public_followup_outcome(
                    response=response,turns=turns,reviews=reviews)
            else:
                assert reviews and reviews[-1].get("disposition") == "complete", reviews[-1:]
        if response["artifacts"]:
            manifests=[result["output"].get("artifact_delivery_manifest") for result in results
                       if isinstance(result.get("output"),dict)]
            proof['artifact_review_statuses']=[manifest.get('review_status') for manifest in manifests
                if isinstance(manifest,dict)]
            # The assertions below independently verify the requested bytes,
            # names, contents, rendering, and source work. An optional review
            # outage is diagnostic; a known unfulfilled requirement still fails.
            assert all(turn.get('response_fulfilled') is not False for turn in turns), (
                'Artifact work has a recorded material failure',
                [turn.get('response_fulfilled') for turn in turns])
        for artifact in response["artifacts"]:
            # Verify an actual rendered download link plus delivered bytes.
            filename = catalogue._artifact_name(artifact)
            if catalogue._artifact_is_image(artifact):
                from playwright.sync_api import expect
                card = page.locator('.image-artifact-card').filter(has_text=Path(filename).name)
                expect(card).to_have_count(1)
                card.scroll_into_view_if_needed()
                expect(card.get_by_role('button',name='Download',exact=True)).to_be_visible()
                image = card.locator('img')
                expect(image).to_have_count(1)
                image.evaluate("image => image.decode()")
                assert image.evaluate("image => image.naturalWidth > 0"), filename
            else:
                assert page.locator('#messages a').filter(has_text=Path(filename).name).count(), filename
        downloads = catalogue._download_and_check_artifacts(base, response["artifacts"],
            scenario=contract, timeout=60, download_dir=output / "downloads")
        if name in {"archive_download_extract_manifest", "archive_tool_fallback_followup_chain"}:
            import csv,io
            from collections import Counter
            from openpyxl import load_workbook
            extraction=next(r['output'] for r in results if r['tool_name']=='archive_extract' and r['status']=='completed')
            expected_count=extraction['entry_count']
            workbook_artifact=next(a for a in response['artifacts'] if catalogue._artifact_name(a).endswith('.xlsx'))
            workbook_bytes,_=catalogue._artifact_bytes(base,workbook_artifact,timeout=60)
            workbook=load_workbook(io.BytesIO(workbook_bytes),read_only=True,data_only=True)
            rows=[r for r in workbook.active.values if any(v is not None for v in r)];workbook.close()
            assert len(rows)-1==expected_count,('Workbook is not the complete current archive',len(rows)-1,expected_count)
            distributions=[]
            for column in zip(*rows[1:]):
                if all(isinstance(value,str) and Path(value).suffix for value in column):
                    assert all('__MACOSX' not in value and not Path(value).name.startswith('._') for value in column)
                    distributions.append(Counter(Path(value).suffix.casefold() for value in column))
            assert distributions,'No actual file identities in delivered workbook'
            proof['archive_inventory']={'expected_rows':expected_count,'delivered_rows':len(rows)-1,
                'complete':True}
        if name == "archive_tool_fallback_followup_chain":
            summary_artifact=next(a for a in response['artifacts'] if catalogue._artifact_name(a).endswith('.csv'))
            summary_bytes,_=catalogue._artifact_bytes(base,summary_artifact,timeout=30)
            summary=list(csv.reader(io.StringIO(summary_bytes.decode('utf-8-sig'))))
            observed=Counter({row[0].casefold():int(row[1]) for row in summary[1:] if row})
            assert sum(observed.values())==expected_count and observed in distributions,('Follow-up counted stale or missing files',observed,distributions,expected_count)
        if name == "html_research_real_images":
            from tests.e2e.live_artifact_assertions import assert_three_practical_itineraries
            artifact=next(a for a in response['artifacts'] if catalogue._artifact_name(a).endswith('.html'))
            body,_=catalogue._artifact_bytes(base,artifact,timeout=30)
            assert_three_practical_itineraries(body)
        if name == "kids_playbook_artifact_pack":
            # A permitted native fallback may create the DOCX. Verify actual
            # page-number fields and embedded media, not a particular method.
            import io,zipfile
            from xml.etree import ElementTree as ET
            artifact=next(a for a in response['artifacts'] if catalogue._artifact_name(a).endswith('.docx'))
            body,_=catalogue._artifact_bytes(base,artifact,timeout=30)
            with zipfile.ZipFile(io.BytesIO(body)) as package:
                fields=[]
                for member in package.namelist():
                    if member.startswith('word/footer') and member.endswith('.xml'):
                        root=ET.fromstring(package.read(member))
                        fields.extend(node.text or '' for node in root.iter() if node.tag.endswith('}instrText'))
                        fields.extend(node.get('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}instr','')
                            for node in root.iter() if node.tag.endswith('}fldSimple'))
                assert any('PAGE' in field.split() for field in fields),'DOCX has no actual page-number field'
                assert any(member.startswith('word/media/') for member in package.namelist())
            proof['docx_page_number_fields_verified']=True
        if name == "spreadsheet_visual_workbook_pack":
            from tests.e2e.live_artifact_assertions import assert_visual_workbook_formulas
            artifact=next(a for a in response['artifacts'] if catalogue._artifact_name(a).endswith('.xlsx'))
            body,_=catalogue._artifact_bytes(base,artifact,timeout=30)
            assert_visual_workbook_formulas(body)
        if name == "formatted_pdf_flyer_pack":
            import io
            import pypdfium2 as pdfium
            from pypdf import PdfReader
            expected_pages = {'community_night_market_poster_STAGE_FORMAT_QA.pdf':1,
                              'community_night_market_memo_STAGE_FORMAT_QA.pdf':2}
            for artifact in response['artifacts']:
                filename = Path(catalogue._artifact_name(artifact)).name
                if filename in expected_pages:
                    body, _ = catalogue._artifact_bytes(base, artifact, timeout=30)
                    assert len(PdfReader(io.BytesIO(body)).pages) == expected_pages[filename], filename
                    # Text extraction can include entirely clipped/off-page
                    # content. Check the rendered page's bounded text too.
                    with pdfium.PdfDocument(body) as document:
                        visible_parts=[]
                        for index in range(len(document)):
                            pdf_page=document[index];text=pdf_page.get_textpage()
                            try:visible_parts.append(text.get_text_bounded())
                            finally:text.close();pdf_page.close()
                    visible=''.join(''.join(visible_parts).split()).upper()
                    assert 'COMMUNITY' in visible and 'STAGEFORMATQA' in visible,(filename,visible)
        proof.update(status="functional_passed", elapsed_ms=samples, tools=sorted(names), downloads=downloads,
                     turns=turns, terminal_frames=[response])
        if contract.max_single_ms is not None:
            catalogue.assess_live_latency(max(samples),contract.max_single_ms,
                label=name+': single request',advisories=proof['latency_advisories'])
        if contract.max_average_ms is not None:
            catalogue.assess_live_latency(sum(samples)/len(samples),contract.max_average_ms,
                label=name+': average request',advisories=proof['latency_advisories'])
        proof['status']='passed'
    except BaseException as error:
        proof.update(status="failed", error=str(error), elapsed_ms=samples, turns=turns,
                     terminal_frames=[f for f in frames if f.get("type") in {"done","error","approval_required"}])
        raise
    finally:
        try:
            page.screenshot(path=str(output / "web.png"), full_page=False, timeout=10000)
        except Exception as error:
            proof["screenshot_error"] = str(error)
        finally:
            (output / "result.json").write_text(json.dumps(proof, indent=2, default=str))


@pytest.mark.parametrize("prompt", [
    "What is happening at Hartford Stage today? https://www.hartfordstage.org/ Report actual productions and dates; no files.",
    "¿Qué se presenta hoy en Hartford Stage? https://www.hartfordstage.org/ Indica las producciones y fechas reales; sin archivos.",
    "Hartford Stage では今日は何が上演されていますか？ https://www.hartfordstage.org/ 実際の作品と日付を教えてください。ファイルは不要です。",
])
def test_live_stage_web_event_entities_in_three_languages(live_web, prompt):
    page, frames, errors, database, conversation, base = live_web
    response, turn, elapsed = _submit(live_web, prompt, timeout=300)
    assert "dracula" in response["text"].lower(), response
    # Interface labels are not extra event subjects in any output language.
    result = response["text"]
    assert "**Main Stage**" not in result and "**Let us choose seats" not in result
    assert not response["artifacts"]
    assert any(t["tool_name"].startswith("browser_") and t["status"] == "completed"
               for t in turn.get("tool_results", [])), turn
    out = Path(os.environ.get("NULLION_LIVE_EVIDENCE_DIR", "/tmp/live-web-evidence")) / conversation.replace(":", "_")
    out.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(out / "web.png"), full_page=False, timeout=10000)
    (out / "result.json").write_text(json.dumps({"status":"passed", "turn":turn,"elapsed_ms":elapsed}, indent=2))

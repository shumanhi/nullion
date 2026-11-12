"""Explicit real-provider replay of saved tasks through stage Web and scheduler."""
import json
import os
from pathlib import Path
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from uuid import uuid4

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, _turns, catalogue

pytestmark = pytest.mark.skipif(
    os.environ.get('NULLION_LIVE_STAGE_E2E') != '1' or not os.environ.get('NULLION_LIVE_CRON_REPLAY_FILE'),
    reason='Explicit stage-only saved-task replay required')


def _diagnostics(conversation):
    settings = json.loads(Path(os.environ['NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION']).read_text())
    request = Request('http://host.docker.internal:' + str(settings['port']) + '/cron-runs?' +
        urlencode({'conversation_id': conversation}), headers={'Authorization': 'Bearer ' + settings['token']})
    from scripts.live_stage_http import read_json
    return read_json(request,timeout=15,retry_seconds=15,opener=urlopen)['events']


def _transport_caption_and_paths(reply):
    from nullion.artifacts import parse_media_directive_line
    lines = reply.splitlines()
    directives = [parse_media_directive_line(line) for line in lines]
    caption = '\n'.join(line for line, directive in zip(lines, directives) if directive is None).strip()
    paths = {str(directive.path) for directive in directives if directive is not None}
    return caption, paths


@pytest.mark.parametrize('kind', ['email', 'imax', 'events', 'social'])
def test_live_saved_cron_finishes_remaining_work_and_persists_evidence(live_web, kind, request):
    page, frames, errors, database, conversation, base = live_web
    source = json.loads(Path(os.environ['NULLION_LIVE_CRON_REPLAY_FILE']).read_text())[kind]
    name = 'Scheduled replay ' + kind + ' ' + uuid4().hex[:8]
    response = page.request.post(base + '/api/crons', data={
        'name': name, 'schedule': source['schedule'], 'schedule_timezone': source.get('schedule_timezone'), 'enabled': False,
        'task': source['task'], 'delivery_channel': 'web', 'delivery_target': conversation,
        'artifact_delivery_options': source.get('artifact_delivery_options') or {}})
    assert response.ok, response.text()
    job = response.json()['job']
    request.node._nullion_saved_background_execution = {'cron_id': job['id'], 'terminal': False}
    assert job['schedule'] == source['schedule']
    if source.get('schedule_timezone'):
        assert job['schedule_timezone'] == source['schedule_timezone'], job
    output = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR']) / ('saved-cron-' + kind)
    output.mkdir(parents=True, exist_ok=True)
    evidence = {'conversation_id': conversation, 'cron_id': job['id'], 'kind': kind}
    terminal = None
    try:
        receipt, _, elapsed = _submit(live_web,
            'Run the saved scheduled task "' + name + '" (ID ' + job['id'] +
            ') once now. Leave its schedule disabled. Deliver the result in this chat.', timeout=180)
        evidence.update(receipt=receipt, setup_elapsed_ms=elapsed)
        # This is an observation window, not a product latency assertion. Do
        # not cancel a progressing saved task merely to clean up the fixture.
        run_started = time.monotonic()
        observation_seconds = float(os.environ.get('NULLION_LIVE_CRON_OBSERVATION_SECONDS', '1200'))
        deadline = run_started + observation_seconds
        provider_guard = request.getfixturevalue('live_stage_provider_guard')
        next_provider_check = run_started
        while time.monotonic() < deadline:
            if time.monotonic() >= next_provider_check:
                blocked = provider_guard.check()
                if blocked:
                    pytest.exit(blocked, returncode=1)
                next_provider_check = time.monotonic() + 15
            try:
                rows = _diagnostics(conversation)
            except (URLError, OSError) as exc:
                evidence.setdefault('evidence_read_errors', []).append(str(exc))
                page.wait_for_timeout(500)
                continue
            evidence['diagnostics'] = rows
            completed = [row for row in rows if row.get('event_type') == 'conversation.cron_run'
                and row.get('cron_id') == job['id']]
            if completed:
                terminal = completed[-1]
                break
            page.wait_for_timeout(500)
        assert terminal is not None, evidence
        assert terminal['response_fulfilled'] is not False and not terminal['report_failed'], terminal
        if terminal['response_fulfilled'] is None:
            assert terminal.get('outcome_review_status') == 'unavailable', terminal
            assert terminal.get('outcome_review_error'), terminal
        assert not terminal['remaining_work'], terminal
        if terminal['source_verification_required'] and terminal['source_verification_satisfied'] is not True:
            # The independent reviewer is a helper. Its outage is diagnostic;
            # required source work and the actual visible delivery below still
            # have to pass. A concrete failure or alert-only decision is strict.
            assert terminal.get('outcome_review_status') == 'unavailable', terminal
            assert terminal.get('outcome_review_error'), terminal
            assert not terminal.get('alert_delivery_decision'), terminal
            assert all(row['status'] == 'completed' for row in terminal['tool_results']), terminal
        tools = terminal['tool_results']
        if kind == 'email':
            assert any(row['tool_name'] == 'calendar_list' and row['status'] == 'completed' for row in tools), terminal
            assert any(row['tool_name'] == 'email_read' and row['status'] == 'completed' for row in tools), terminal
        assert any(row['event_type'] == 'conversation.model_timing' for row in rows), rows
        assert any(row['event_type'] == 'conversation.tool_timing' for row in rows), rows
        if terminal['alert_delivery_decision'] != 'silent':
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    delivered = [row for row in _turns(database, conversation) if row.get('cron_id') == job['id']]
                except (URLError, OSError) as exc:
                    evidence.setdefault('evidence_read_errors', []).append(str(exc))
                    page.wait_for_timeout(300)
                    continue
                if delivered:
                    evidence['delivered'] = delivered
                    expected, attachment_paths = _transport_caption_and_paths(delivered[-1]['assistant_reply'])
                    assert expected
                    # The setup acknowledgement is also a visible bot message.
                    # Prove that the actual completed cron report arrives once,
                    # rather than accepting any nonempty message container.
                    page.wait_for_function("""expected => Array.from(
                        document.querySelectorAll('#messages .msg.bot .bubble'))
                        .some(bubble => (bubble.getAttribute('data-raw-text') || '').trim() === expected)""",
                        arg=expected, timeout=15000)
                    matches = [bubble for bubble in page.locator('#messages .msg.bot .bubble').all()
                        if (bubble.get_attribute('data-raw-text') or '').strip() == expected]
                    assert len(matches) == 1 and matches[0].is_visible()
                    evidence['visible_report_raw_text'] = matches[0].get_attribute('data-raw-text')
                    assert '**' not in matches[0].inner_text()
                    required = set((source.get('artifact_delivery_options') or {}).get('required_artifact_extensions') or ())
                    if required:
                        history = page.request.get(base + '/api/chat/history/' + conversation).json()['messages']
                        reports = [message for message in history if message.get('role') == 'bot'
                                   and message['text'].strip() == expected]
                        assert len(reports) == 1, reports
                        artifacts = reports[0].get('artifacts') or []
                        assert attachment_paths == {artifact['path'] for artifact in artifacts}, artifacts
                        assert required.issubset(catalogue._artifact_extensions(artifacts)), artifacts
                        downloads = []
                        for artifact in artifacts:
                            filename = Path(catalogue._artifact_name(artifact)).name
                            if Path(filename).suffix.lower() not in required:
                                continue
                            body, content_type = catalogue._artifact_bytes(base, artifact, timeout=30)
                            assert body, filename
                            (output / filename).write_bytes(body)
                            # HTML has separate Open and Download links. The
                            # selected attachment's download must occur once.
                            download = page.locator('#messages a[href=' + json.dumps(artifact['url']) + ']')
                            assert download.count() == 1 and filename in download.inner_text(), filename
                            if Path(filename).suffix.lower() == '.html':
                                visible, _ = catalogue._validate_html_visual_content(filename, body)
                                for token in (source.get('artifact_delivery_options') or {}).get('required_artifact_content_tokens') or ():
                                    assert ''.join(token.casefold().split()) in ''.join(visible.casefold().split()), (filename, token)
                            downloads.append({'name': filename, 'bytes': len(body), 'content_type': content_type})
                        evidence['downloaded_artifacts'] = downloads
                    break
                page.wait_for_timeout(300)
            else:
                pytest.fail('Completed run has no durable Web delivery receipt')
        assert not errors, errors
    except BaseException as exc:
        evidence['error'] = str(exc)
        raise
    finally:
        evidence['terminal'] = terminal
        if 'run_started' in locals():
            evidence['run_observation_elapsed_ms'] = round((time.monotonic() - run_started) * 1000, 1)
        # Save the actual workflow failure before visual evidence. A crashed
        # renderer must not replace that diagnosis with a screenshot exception.
        (output / 'result.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
        try:
            page.screenshot(path=str(output / 'web.png'), full_page=False, timeout=10000)
        except Exception as exc:
            evidence['screenshot_error'] = str(exc)
            (output / 'result.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
            if not evidence.get('error'):
                pytest.fail('Workflow completed but the Web evidence renderer failed: ' + str(exc))
        # Keep a still-running owned job for diagnosis; never delete a live run.
        if terminal is not None:
            request.node._nullion_saved_background_execution['terminal'] = True
            deleted = page.request.delete(base + '/api/crons/' + job['id'])
            assert deleted.ok, deleted.text()

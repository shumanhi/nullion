#!/usr/bin/env python3
"""Opt-in stage API replay of a disabled, isolated matches-only monitor."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import statistics
import time
from uuid import uuid4

import httpx


def validate_recorded_run(report: dict, db: Path) -> dict:
    """Validate persisted API results without repeating external source checks."""
    with sqlite3.connect(db.resolve().as_uri() + '?mode=ro', uri=True) as conn:
        for row in report['plain_turns'] + [report['adjacent_scheduler'], report['manual_run_request']]:
            saved = [json.loads(raw) for raw, in conn.execute(
                "SELECT payload FROM conversation_events WHERE json_extract(payload,'$.conversation_id')=? ORDER BY updated_at",
                (row['conversation_id'],))]
            chat = [e for e in saved if e.get('event_type') == 'conversation.chat_turn' and e.get('source') != 'cron']
            assert chat, row
            row['tools'] = [{'name': t.get('tool_name'), 'status': t.get('status')}
                            for t in chat[-1].get('tool_results', [])]
            if row in report['plain_turns']:
                assert not row['tools'], row
                assert sum(e.get('event_type') == 'conversation.model_timing' for e in saved) == 1
            if row is report['manual_run_request']:
                report['browser_timings'] = [e for e in saved if e.get('event_type') == 'conversation.tool_timing'
                                             and str(e.get('tool_name') or '').startswith('browser_')]
                report['completion_reviews'] = [e for e in saved if e.get('event_type') == 'conversation.completion_review']
                assert report['browser_timings']
                completed = [e['payload'] for e in report['cron_events'] if e.get('payload', {}).get('stage') == 'single_agent_done']
                assert completed and completed[-1].get('alert_delivery_mode') == 'verified_matches_only'
                assert completed[-1].get('source_verification_required') is True
                report['source_verification'] = completed[-1]
        assert any(t['name'] == 'list_crons' for t in report['adjacent_scheduler']['tools'])
        assert any(t['name'] == 'run_cron' for t in report['manual_run_request']['tools'])
    report['simple_latency'] = {
        'average_seconds': statistics.mean(r['seconds'] for r in report['plain_turns']),
        'max_seconds': max(r['seconds'] for r in report['plain_turns'])}
    assert report['simple_latency']['average_seconds'] < 12
    assert report['simple_latency']['max_seconds'] < 25
    assert report['terminal_turns'] and all(t.get('assistant_reply') for t in report['terminal_turns'])
    report['monitor_verified'] = report['source_verification'].get('source_verification_satisfied') is True
    report['status'] = 'passed'
    return report


def run(job_file: Path, output: Path) -> dict:
    if os.environ.get('NULLION_LIVE_STAGE_E2E') != '1':
        raise SystemExit('Set NULLION_LIVE_STAGE_E2E=1 for this stage-only replay.')
    job = json.loads(job_file.read_text())
    db = Path.home() / '.nullion-stage/runtime.db'
    run_id = uuid4().hex[:10]
    conversation = f'web:conditional-monitor-probe:{run_id}'
    report = {'lane': 'stage', 'conversation_id': conversation, 'plain_turns': []}
    created_id = ''

    def read_events(table, field, value):
        with sqlite3.connect(db.resolve().as_uri() + '?mode=ro', uri=True) as conn:
            return [json.loads(raw) for raw, in conn.execute(
                f"SELECT payload FROM {table} WHERE json_extract(payload, ?) = ? ORDER BY updated_at",
                (field, value))]

    def persist():
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        output.chmod(0o600)

    with httpx.Client(base_url='http://127.0.0.1:8753', timeout=180) as client:
        def send(prompt, name):
            cid = f'{conversation}:{name}'
            start = time.monotonic()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
            response.raise_for_status()
            payload = response.json()
            assert payload.get('type') not in {'error', 'approval_required'}, payload
            return {'conversation_id': cid, 'seconds': time.monotonic() - start, 'response': payload}

        for index, prompt in enumerate(['Return only 2+2.', 'Solo 3+2.', '数字だけ：4+2。']):
            report['plain_turns'].append(send(prompt, f'plain-{index}'))
        report['adjacent_scheduler'] = send('List my scheduled tasks.', 'scheduler')
        try:
            response = client.post('/api/crons', json={
                'name': f'Stage monitor replay {run_id}', 'schedule': '0 0 1 1 *', 'enabled': False,
                'task': job['task'], 'workspace_id': 'workspace_admin', 'delivery_channel': 'web',
                'delivery_target': conversation, 'artifact_delivery_options': {
                    **job.get('artifact_delivery_options', {}), 'alert_delivery_mode': 'verified_matches_only'}})
            response.raise_for_status()
            created_id = response.json()['job']['id']
            report['fixture_cron_id'] = created_id
            report['manual_run_request'] = send(f'Run scheduled task {created_id} once now.', 'manual')
            persist()
            deadline = time.monotonic() + 480
            while time.monotonic() < deadline:
                events = read_events('runtime_events', '$.payload.cron_id', created_id)
                report['cron_events'] = events
                terminal = [e for e in events if e.get('event_type') in {
                    'cron.delivery.sent', 'cron.delivery.saved', 'cron.delivery.failed', 'cron.delivery.silent'}]
                if terminal:
                    break
                time.sleep(1)
            assert terminal, 'No terminal delivery event within 480 seconds.'
            turns = read_events('conversation_events', '$.cron_id', created_id)
            report['terminal_turns'] = turns
            assert turns and all(t.get('assistant_reply') for t in turns), 'Manual run lost its terminal response.'
            validate_recorded_run(report, db)
        finally:
            persist()
            if created_id:
                # The isolated fixture is disabled; cleanup never changes other jobs.
                response = client.delete(f'/api/crons/{created_id}')
                response.raise_for_status()
                report['fixture_removed'] = response.json().get('ok') is True
                persist()
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = run(args.job_file, args.output)
    print(json.dumps({'status': result['status'], 'simple_latency': result['simple_latency'],
                      'fixture_removed': result.get('fixture_removed')}))

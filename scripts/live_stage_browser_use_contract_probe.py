#!/usr/bin/env python3
"""Opt-in stage API proof for visual navigation, PNG delivery and idle latency."""
from __future__ import annotations
import argparse
import hashlib
import json
import sqlite3
import time
import urllib.request
from pathlib import Path
from uuid import uuid4


def run(base_url: str, db: Path, output: Path, rental_only: bool = False):
    previous = json.loads(output.read_text()) if rental_only and output.exists() else {}
    records = [record for record in previous.get('records', []) if record['name'] != 'rental']
    samples = [record['elapsed_seconds'] for record in records if record['name'].startswith('plain-')]
    def request(name, text, timeout=720):
        conversation = f'web:browser-use-live-{uuid4().hex}'
        started = time.monotonic()
        output.write_text(json.dumps({'records': records, 'pending_request': {'name': name, 'conversation_id': conversation, 'started_at': time.time()}}, ensure_ascii=False, indent=2, default=str))
        print(name, 'started', conversation, flush=True)
        payload = json.dumps({'text': text, 'conversation_id': conversation}).encode()
        req = urllib.request.Request(base_url + '/api/chat', data=payload, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            result = json.load(response)
        elapsed = time.monotonic() - started
        # The API can finish before its delayed SQLite checkpoint lands.
        # Wait for this exact turn rather than accepting an empty tool list.
        deadline = time.monotonic() + 20
        while True:
            with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as connection:
                events = [json.loads(row[0]) for row in connection.execute("SELECT payload FROM conversation_events WHERE collection='conversation_events' AND json_extract(payload, '$.conversation_id')=? ORDER BY json_extract(payload, '$.created_at')", (conversation,))]
            turns = [event for event in events if event.get('event_type') == 'conversation.chat_turn']
            if turns or time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        assert turns, f'No stored turn for {conversation}'
        tools = turns[-1].get('tool_results', []) if turns else []
        record = {'name': name, 'conversation_id': conversation, 'elapsed_seconds': round(elapsed, 3), 'text': result.get('text'), 'artifacts': result.get('artifacts'), 'tools': tools,
            'timings': [e for e in events if e.get('event_type') in ('conversation.tool_timing', 'conversation.model_timing', 'conversation.latency_timing')]}
        records.append(record)
        output.write_text(json.dumps({'records': records}, ensure_ascii=False, indent=2, default=str))
        print(name, round(elapsed, 2), [tool.get('tool_name') for tool in tools], flush=True)
        return result, tools, elapsed
    if not rental_only:
        for index, text in enumerate(('Reply briefly: hello.', 'Responde brevemente: hola.', '短くこんにちはと答えてください。')):
            result, tools, elapsed = request(f'plain-{index}', text, timeout=120)
            assert result.get('text') and not tools, result
            samples.append(elapsed)
        result, tools, elapsed = request('adjacent-weather', 'What is the weather in New York City today?', timeout=180)
        assert any(tool.get('tool_name') == 'weather_forecast' and tool.get('status') == 'completed' for tool in tools), result
    url = 'https://www.kayak.com/cars/NYC-a15830/2026-10-10-08h/2026-10-10-21h?sort=price_a'
    result, tools, elapsed = request('rental', f'Find up to three actual gasoline/non-electric sedan rental listings for Saturday October 10, 2026, pickup 8 AM and return 9 PM, driver age 39, near the Upper West Side in NYC. Compare displayed total prices, provider and pickup location. Use the installed Browser Use visual navigation tool on {url}. Keep useful listings even when fuel type, age eligibility, taxes or return policy are unshown, and state those unknowns. Attach the captured PNG evidence. Do not book or sign in.')
    outputs = [tool.get('output', {}) for tool in tools if tool.get('tool_name') == 'browser_run_task' and tool.get('status') == 'completed']
    assert outputs and outputs[0].get('observations'), {'text': result.get('text'), 'tool_statuses': [(tool.get('tool_name'), tool.get('status'), tool.get('error')) for tool in tools]}
    assert result.get('artifacts'), result
    captures = []
    for artifact in result['artifacts']:
        route = artifact.get('download_url') or artifact.get('url')
        if route:
            with urllib.request.urlopen(route if route.startswith('http') else base_url + route, timeout=20) as response:
                data = response.read()
        else:
            data = Path(artifact['path']).read_bytes()
        assert data.startswith(b'\x89PNG\r\n\x1a\n'), artifact
        captures.append({'name': artifact['name'], 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    assert 'task is still open' not in result['text'].lower()
    assert '$' in result['text'], result
    assert sum(samples) / len(samples) < 20 and max(samples) < 40, samples
    report = {'records': records, 'acceptance': {'plain_average_seconds': sum(samples)/len(samples), 'plain_p95_seconds': max(samples), 'rental_elapsed_seconds': elapsed, 'captures': captures, 'passed': True}}
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-url', default='http://127.0.0.1:8753')
    parser.add_argument('--db', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rental-only', action='store_true')
    args = parser.parse_args()
    run(args.base_url.rstrip('/'), args.db, args.output, args.rental_only)

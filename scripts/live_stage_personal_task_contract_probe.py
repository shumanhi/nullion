#!/usr/bin/env python3
"""Opt-in read-only stage task recall, adjacent weather, and plain-chat latency."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3

from live_stage_chat_contract_probe import SCENARIOS, Scenario, main as run_contracts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:8753')
    parser.add_argument('--runtime-db', type=Path, default=Path.home() / '.nullion-stage/runtime.db')
    args = parser.parse_args(argv)
    if os.environ.get('NULLION_LIVE_STAGE_E2E') != '1':
        parser.error('Set NULLION_LIVE_STAGE_E2E=1 only after stage deployment and live quota are authorized.')
    if not args.runtime_db.is_file():
        parser.error('The stage runtime database is required for backing tool receipts.')
    uri = f'file:{args.runtime_db}?mode=ro'
    with sqlite3.connect(uri, uri=True) as connection:
        last_row = connection.execute('SELECT COALESCE(MAX(rowid),0) FROM conversation_events').fetchone()[0]
    SCENARIOS['personal_tasks_recall'] = Scenario(
        name='personal_tasks_recall', prompt='Whats in my todo list? Read the persistent list without changing anything.',
        max_average_ms=15000, max_single_ms=30000, required_tool_all=('personal_tasks_list',),
        forbidden_tool_any=('personal_task_save', 'create_cron', 'update_cron', 'toggle_cron', 'delete_cron', 'run_cron'),
        require_no_artifacts=True)
    result = run_contracts(['--base-url', args.base_url, '--runtime-db', str(args.runtime_db), '--repeat', '3',
                            '--scenario', 'simple', '--scenario', 'weather', '--scenario', 'personal_tasks_recall'])
    if result:
        return result
    with sqlite3.connect(uri, uri=True) as connection:
        rows = connection.execute("SELECT payload FROM conversation_events WHERE rowid>? "
            "AND json_extract(payload,'$.event_type')='conversation.chat_turn' "
            "AND json_extract(payload,'$.conversation_id') LIKE ?", (last_row, 'web:live-stage-contract:personal_tasks_recall:%')).fetchall()
    receipts = [tool for row in rows for tool in json.loads(row[0]).get('tool_results', [])
                if tool.get('tool_name') == 'personal_tasks_list']
    assert len(receipts) >= 3, 'Missing task-list tool evidence for the real chat requests.'
    assert all(tool.get('status') == 'completed' and tool.get('output', {}).get('persisted') is True
               for tool in receipts), 'Task recall did not obtain persisted list receipts.'
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

"""Opt-in local failure evidence while a long container suite is running."""
import json
import os
from pathlib import Path


def pytest_runtest_logreport(report):
    root = os.environ.get('NULLION_FAILURE_EVIDENCE_DIR')
    if not root or not report.failed:
        return
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / (str(os.getpid()) + '.jsonl')).open('a') as stream:
        stream.write(json.dumps({'nodeid': report.nodeid, 'when': report.when,
            'longrepr': str(report.longrepr)}, ensure_ascii=False) + '\n')

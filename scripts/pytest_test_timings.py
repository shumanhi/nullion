"""Passive pytest reports to persistent, per-process timing files."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import platform
import time
from uuid import uuid4

from scripts import test_performance as timing


class TimingRecorder:
    def __init__(self, config):
        self.root=Path(config.rootpath)
        self.policy={}
        policy=self.root/'tests/e2e/live_stage_catalog.json'
        if policy.is_file():self.policy=json.loads(policy.read_text())
        self.run_id=os.environ.get('NULLION_TEST_RUN_ID') or uuid4().hex
        self.session_id=uuid4().hex
        directory=Path(os.environ.get('NULLION_TEST_PERFORMANCE_DIR') or self.root/'.e2e-artifacts/test-performance')
        self.path=directory/(self.session_id+'.jsonl')
        self.pending={};self.attempts={};self.source_hashes={};self.error=None;self.recorded=0
        self.metadata={'record_type':'run','schema_version':1,'run_id':self.run_id,'session_id':self.session_id,
                       'started_at':timing.now(),'revision':os.environ.get('NULLION_TEST_REVISION') or None,
                       'candidate_revision':os.environ.get('NULLION_TEST_CANDIDATE_REVISION') or None,
                       'dirty':os.environ.get('NULLION_TEST_DIRTY')=='1' if os.environ.get('NULLION_TEST_DIRTY') in ('0','1') else None,
                       'provider':os.environ.get('NULLION_TEST_PROVIDER') or None,
                       'model':os.environ.get('NULLION_TEST_MODEL') or None,
                       'runner_platform':platform.system()+'-'+platform.machine(),
                       'python':platform.python_version(),'runner_image':os.environ.get('NULLION_E2E_IMAGE') or None,
                       'measurement_source':'pytest_report','worker':os.environ.get('PYTEST_XDIST_WORKER')}
        parallel=os.environ.get('PYTEST_XDIST_WORKER_COUNT')
        self.metadata['parallel_workers']=int(parallel) if parallel and parallel.isdigit() else (
            None if hasattr(config,'workerinput') else 1)
        self.write(self.metadata)

    def write(self, row):
        if self.error:return
        try:timing.append(self.path,row)
        except (OSError,ValueError,TypeError) as exc:self.error=str(exc)

    def pytest_runtest_logstart(self, nodeid, location):
        self.write({'record_type':'case_started','nodeid':nodeid,'recorded_at':timing.now()})

    def pytest_runtest_logreport(self, report):
        key=report.nodeid
        if report.when=='setup' and key in self.pending:self.finish(key,complete=False)
        if key not in self.pending:
            self.attempts[key]=self.attempts.get(key,0)+1
            self.pending[key]={'phases':{},'outcomes':{},'started':report.start,
                               'xfail':False,'properties':{}}
        case=self.pending[key]
        case['phases'][report.when]=report.duration
        case['outcomes'][report.when]=report.outcome
        case['xfail'] |= hasattr(report,'wasxfail')
        case['stopped']=report.stop
        case['properties'].update({name:value for name,value in report.user_properties
                                   if isinstance(value,(int,float)) and math.isfinite(value)})
        if report.when=='teardown':self.finish(key,complete=True)

    def finish(self, nodeid, *, complete):
        case=self.pending.pop(nodeid)
        complete=complete and not ('call' not in case['phases'] and case['outcomes'].get('setup')=='passed')
        outcomes=case['outcomes'].values()
        if not complete:outcome='incomplete'
        elif 'failed' in outcomes:outcome='failed'
        elif 'rerun' in outcomes:outcome='rerun'
        elif case['xfail']:outcome='xfailed' if 'skipped' in outcomes else 'xpassed'
        elif 'skipped' in outcomes:outcome='skipped'
        else:outcome='passed'
        file=nodeid.split('::',1)[0]
        if file not in self.source_hashes:
            source=self.root/file
            self.source_hashes[file]=hashlib.sha256(source.read_bytes()).hexdigest() if source.is_file() else None
        labels=timing.classify(nodeid,self.policy)
        lane=os.environ.get('NULLION_TEST_LANE') or (
            'stage' if os.environ.get('NULLION_LIVE_STAGE_E2E')=='1'
            and labels['kind'] in ('live_workflow','saved_replay','aggregate_entry_point') else 'local')
        attempt=self.attempts[nodeid]
        stopped=case['stopped'] if complete else time.time()
        self.write({'record_type':'case','measurement_id':f'{self.session_id}:{nodeid}:{attempt}',
                    'nodeid':nodeid,'attempt':attempt,'lane':lane,**labels,
                    'recorded_at':timing.now(),'outcome':outcome,'complete':complete,
                    'source_sha256':self.source_hashes[file],
                    'phase_seconds':case['phases'],'call_seconds':case['phases'].get('call'),
                    'total_seconds':sum(case['phases'].values()) if complete else None,
                    'elapsed_seconds':max(0,stopped-case['started']),
                    'metrics':case['properties']})
        self.recorded+=1

    def pytest_sessionfinish(self, session, exitstatus):
        for nodeid in list(self.pending):self.finish(nodeid,complete=False)
        self.write({'record_type':'end','finished_at':timing.now(),'exitstatus':int(exitstatus),
                    'recorded_cases':self.recorded})

    def pytest_terminal_summary(self, terminalreporter):
        terminalreporter.write_line('Timing history unavailable: '+self.error if self.error
                                   else f'Timing history: {self.recorded} cases saved to {self.path}')


def pytest_configure(config):
    # xdist forwards reports to its controller. Only workers persist them, so
    # one executed test cannot become two measurements of the same attempt.
    if getattr(config.option,'numprocesses',None) and not hasattr(config,'workerinput'):
        return
    if not config.option.collectonly and os.environ.get('NULLION_TEST_PERFORMANCE_DISABLE')!='1':
        config.pluginmanager.register(TimingRecorder(config),'nullion-test-timings')

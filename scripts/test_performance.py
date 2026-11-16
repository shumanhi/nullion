#!/usr/bin/env python3
"""Local append-only test timing history and comparisons. Never run tests."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY = ROOT/'.e2e-artifacts/test-performance'


def now():
    return datetime.now(timezone.utc).isoformat()


def classification(nodeid, root=ROOT):
    path = root/'tests/e2e/live_stage_catalog.json'
    policy = json.loads(path.read_text()) if path.exists() else {}
    return classify(nodeid, policy)


def classify(nodeid, policy):
    selector = nodeid.split('[',1)[0]
    for retired in policy.get('retired_selectors',[]):
        if retired['selector']==selector:
            return {'category':retired['category'],'kind':retired['kind']}
    filename = selector.split('::',1)[0].rsplit('/',1)[-1]
    info = {**policy.get('modules',{}).get(filename,{}),
            **policy.get('test_overrides',{}).get(selector,{})}
    scenario_roots = {'tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario',
                      'tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue'}
    if selector in scenario_roots and '[' in nodeid:
        scenario = nodeid.split('[',1)[1].rsplit(']',1)[0]
        for category,names in policy.get('scenario_categories',{}).items():
            if scenario in names:
                info['category']=category
                break
    return {'category':info.get('category','local_tests'),
            'kind':info.get('kind','local_e2e' if nodeid.startswith('tests/e2e/') else 'unit')}


def append(path, record):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    with path.open('a',encoding='utf-8') as stream:
        stream.write(json.dumps(record,ensure_ascii=False,allow_nan=False)+'\n')
    path.chmod(0o600)


def history(directory):
    samples=[]; seen=set(); malformed=0
    for path in sorted(Path(directory).glob('*.jsonl')):
        metadata={}
        for line in path.read_bytes().splitlines():
            try:
                row=json.loads(line)
                if row.get('record_type')=='run':
                    metadata=row
                elif row.get('record_type')=='case':
                    identifier=row['measurement_id']
                    if identifier not in seen:
                        seen.add(identifier)
                        samples.append({**metadata,**row})
            except (ValueError,KeyError,TypeError,AttributeError):
                malformed+=1
    return samples,malformed


def compare(samples, *, window=3, metric='call_seconds'):
    groups=defaultdict(list)
    # Revisions deliberately vary over time. Environment/measurement modes do not.
    environment_keys=('lane','kind','provider','model','runner_platform','python','runner_image','parallel_workers','measurement_source')
    for sample in samples:
        key=(sample['nodeid'],*(sample.get(k) for k in environment_keys))
        groups[key].append(sample)
    result=[]
    for key,rows in groups.items():
        rows.sort(key=lambda r:(r.get('recorded_at',''),r['measurement_id']))
        successes=[r for r in rows if r.get('outcome')=='passed'
                   and isinstance(r.get(metric),(int,float)) and math.isfinite(r[metric]) and r[metric]>=0]
        values=[r[metric] for r in successes]
        count=min(window,len(values)//2)
        baseline=statistics.median(values[-count*2:-count]) if count else None
        recent=statistics.median(values[-count:]) if count else None
        delta=(recent-baseline)/baseline*100 if baseline not in (None,0) else None
        latest=rows[-1]
        result.append({'nodeid':key[0], 'environment':dict(zip(environment_keys,key[1:])),
                       'category':latest.get('category'), 'samples':len(rows),
                       'successful_samples':len(successes),
                       'failed_samples':sum(r.get('outcome')=='failed' for r in rows),
                       'incomplete_samples':sum(r.get('outcome')=='incomplete' for r in rows),
                       'retried_samples':sum(r.get('outcome')=='rerun' for r in rows),
                       'skipped_samples':sum(r.get('outcome') in ('skipped','xfailed') for r in rows),
                       'latest_outcome':latest.get('outcome'), 'latest_seconds':latest.get(metric),
                       'latest_elapsed_seconds':latest.get('elapsed_seconds'),
                       'latest_revision':latest.get('revision'),
                       'comparison_samples_per_window':count,
                       'baseline_median_seconds':baseline,'recent_median_seconds':recent,
                       'change_percent':delta,
                       'observed_p95_seconds':sorted(values)[math.ceil(.95*len(values))-1] if values else None,
                       'test_source_changed':len({r.get('source_sha256') for r in rows if r.get('source_sha256')})>1})
    return sorted(result,key=lambda r:r.get('latest_seconds') or 0,reverse=True)


def import_live_log(path, directory, *, revision=None, lane='stage'):
    """Import measured legacy case logs without inventing revision or setup time."""
    content=Path(path).read_bytes()
    destination=Path(directory)/('import-'+hashlib.sha256(content).hexdigest()[:24]+'.jsonl')
    if destination.exists():
        return {'file':str(destination),'imported':0,'already_imported':True}
    rows=[]
    for line in content.decode().splitlines():
        row=json.loads(line)
        if row.get('phase')!='call' and row.get('outcome') not in ('failed','skipped'):
            continue
        duration=row.get('duration_seconds')
        if not isinstance(duration,(int,float)) or not math.isfinite(duration) or duration<0:
            raise ValueError('Legacy timing is missing a valid duration')
        recorded=datetime.fromtimestamp(row['recorded_at'],timezone.utc).isoformat()
        # Copied or incrementally re-imported legacy files cannot multiply a sample.
        identity=json.dumps([row['nodeid'],row['recorded_at'],duration,row['phase'],row['outcome']])
        rows.append({'record_type':'case','measurement_id':'legacy-'+hashlib.sha256(identity.encode()).hexdigest(),
                     'nodeid':row['nodeid'],'attempt':None,'recorded_at':recorded,
                     'outcome':row['outcome'],'complete':True,
                     'call_seconds':duration if row['phase']=='call' else None,
                     'total_seconds':None,'elapsed_seconds':None,
                     'phase_seconds':{row['phase']:duration},**classification(row['nodeid'])})
    append(destination,{'record_type':'run','schema_version':1,'run_id':destination.stem,
                        'started_at':min((r['recorded_at'] for r in rows),default=now()),
                        'revision':revision,'lane':lane,'measurement_source':'legacy_call',
                        'runner_platform':'unknown','python':'unknown','runner_image':'unknown',
                        'provider':None,'model':None,'source_log':Path(path).name,
                        'provenance_note':'Historical measured calls; runtime revision/environment may be unknown or mixed.'})
    for row in rows:append(destination,row)
    return {'file':str(destination),'imported':len(rows),'already_imported':False}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--history',type=Path,default=DEFAULT_HISTORY)
    parser.add_argument('--test',help='Exact test nodeid or function selector')
    parser.add_argument('--lane')
    parser.add_argument('--kind')
    parser.add_argument('--window',type=int,default=3)
    parser.add_argument('--limit',type=int,default=20)
    parser.add_argument('--metric',choices=('call_seconds','total_seconds'),default='call_seconds')
    parser.add_argument('--json',action='store_true')
    parser.add_argument('--import-live-log',type=Path)
    parser.add_argument('--revision',help='Known test revision for an explicit legacy import only')
    args=parser.parse_args(argv)
    if args.window<1 or args.limit<1:parser.error('window and limit must be positive')
    if args.import_live_log:
        print(json.dumps(import_live_log(args.import_live_log,args.history,
                                         revision=args.revision,lane=args.lane or 'stage'),indent=2))
        return 0
    samples,malformed=history(args.history)
    samples=[r for r in samples if (not args.test or r['nodeid']==args.test or r['nodeid'].split('[',1)[0]==args.test)
             and (not args.lane or r.get('lane')==args.lane) and (not args.kind or r.get('kind')==args.kind)]
    rows=compare(samples,window=args.window,metric=args.metric)
    if args.json:
        print(json.dumps({'metric':args.metric,'samples':len(samples),'malformed_records':malformed,
                          'comparisons':rows[:args.limit]},ensure_ascii=False,indent=2))
    else:
        print(f'Measured samples: {len(samples)}; malformed records ignored: {malformed}; metric: {args.metric}')
        print('Latest(s)  Baseline(s)  Recent(s)  Change     Pass/Fail  Test')
        for r in rows[:args.limit]:
            def number(value):return '-' if value is None else f'{value:.3f}'
            change='needs baseline' if r['change_percent'] is None else f"{r['change_percent']:+.1f}%"
            print(f"{number(r['latest_seconds']):>9}  {number(r['baseline_median_seconds']):>11}  "
                  f"{number(r['recent_median_seconds']):>9}  {change:>14}  "
                  f"{r['successful_samples']}/{r['failed_samples']} {r['latest_outcome']}  "
                  f"{r['nodeid']} [{r['environment']['lane']}; {r['environment']['measurement_source']}]")
    return 0


if __name__=='__main__':raise SystemExit(main())

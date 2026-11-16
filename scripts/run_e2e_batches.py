#!/usr/bin/env python3
"""Run the collected E2E modules in bounded, isolated pytest processes."""
from __future__ import annotations

import argparse
from collections import OrderedDict
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from uuid import uuid4


def pytest_collection_finish(session):
    destination=os.environ.get('NULLION_E2E_COLLECTION_MANIFEST')
    if destination:
        Path(destination).write_text(json.dumps([
            {'nodeid':item.nodeid,'path':str(item.path)} for item in session.items]))


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-files',type=int,default=40)
    parser.add_argument('pytest_args',nargs=argparse.REMAINDER)
    args=parser.parse_args(argv)
    if args.batch_files<1:parser.error('batch-files must be positive')
    values=args.pytest_args
    if values[:1]==['--']:values=values[1:]
    values=values or ['tests/e2e']
    boundary=next((i for i,value in enumerate(values) if value.startswith('-')),len(values))
    if not boundary:parser.error('put test paths before pytest options')
    options=values[boundary:]
    root=Path.cwd()
    evidence=Path(os.environ.get('NULLION_E2E_BATCH_EVIDENCE',root/'.e2e-artifacts/batches'))
    evidence.mkdir(parents=True,exist_ok=True)
    run=evidence/uuid4().hex;run.mkdir()
    manifest=run/'collection.json'
    env={**os.environ,'NULLION_E2E_COLLECTION_MANIFEST':str(manifest)}
    with (run/'collection.log').open('w') as log:
        status=subprocess.run([sys.executable,'-m','pytest','-qq','--collect-only',
            '-p','scripts.run_e2e_batches',*values],env=env,stdout=log,stderr=subprocess.STDOUT).returncode
    if status:
        print((run/'collection.log').read_text()[-8000:],flush=True)
        return status
    records=json.loads(manifest.read_text())
    if not records or len({row['nodeid'] for row in records})!=len(records):
        raise RuntimeError('Collection must contain nonempty, unique test identities')
    modules=OrderedDict()
    for row in records:modules.setdefault(row['path'],[]).append(row['nodeid'])
    paths=list(modules);results=[]
    run_id=os.environ.get('NULLION_TEST_RUN_ID') or uuid4().hex
    for index in range(0,len(paths),args.batch_files):
        files=paths[index:index+args.batch_files]
        expected=[node for path in files for node in modules[path]]
        number=index//args.batch_files+1
        print(f'E2E batch {number}: {len(files)} modules, {len(expected)} collected cases',flush=True)
        with tempfile.TemporaryDirectory(prefix='nullion-e2e-batch-') as home:
            batch_env={**os.environ,'HOME':home,'NULLION_HOME':str(Path(home)/'.nullion'),
                'NULLION_ENV_FILE':str(Path(home)/'none.env'),'NULLION_TEST_RUN_ID':run_id,
                'XDG_CACHE_HOME':str(Path(home)/'cache'),'XDG_CONFIG_HOME':str(Path(home)/'config'),
                'XDG_DATA_HOME':str(Path(home)/'data')}
            actual=run/f'batch-{number}.json'
            batch_env['NULLION_E2E_COLLECTION_MANIFEST']=str(actual)
            process=subprocess.Popen([sys.executable,'-m','pytest','-q','-p','scripts.run_e2e_batches',*files,*options],
                env=batch_env,start_new_session=True)
            try:status=process.wait()
            finally:
                # This process group belongs solely to the completed batch.
                # Drop orphaned browser children before the next batch starts.
                try:os.killpg(process.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                process.wait()
            if not actual.is_file() or [r['nodeid'] for r in json.loads(actual.read_text())]!=expected:
                raise RuntimeError('Batch collection differs from the original native collection')
        results.append({'batch':number,'nodeids':expected,'exit_code':status})
        (run/'results.json').write_text(json.dumps({'collected':len(records),
            'collection_only':'--collect-only' in options,'batches':results},indent=2))
        if status:return status
    return 0


if __name__=='__main__':
    raise SystemExit(main())

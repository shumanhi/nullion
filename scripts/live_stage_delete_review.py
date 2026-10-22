"""Opt-in live stage status/denial replay. Never executes the deletion command."""
import json
import os
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def main():
    if os.environ.get('NULLION_LIVE_STAGE_E2E') != '1':
        raise SystemExit('Set NULLION_LIVE_STAGE_E2E=1 for the stage replay')
    home=Path.home()/'.nullion-stage'
    os.environ['NULLION_HOME']=str(home)
    os.environ['NULLION_WORKSPACE_STORAGE_ROOT']=str(home/'workspaces')
    os.environ['NULLION_TERMINAL_ENABLED']='1'
    deadline=time.monotonic()+20
    while True:
        try:
            with urlopen('http://127.0.0.1:8753/api/health',timeout=1) as response:
                if response.status==200:
                    break
        except (OSError,URLError):
            if time.monotonic()>=deadline:
                raise
            time.sleep(0.2)
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolExecutor,ToolInvocation,create_core_tool_registry
    from nullion.users import messaging_delivery_targets_for_workspace
    runtime=bootstrap_persistent_runtime(home/'runtime.db')
    nonce=uuid4().hex
    workspace='workspace_validation_'+nonce
    assert not messaging_delivery_targets_for_workspace(workspace)
    folder=home/'workspaces'/'workspace_admin'/'scratch'/('delete-review-'+nonce)
    folder.mkdir(parents=True)
    file=folder/'画像.txt';file.write_text('verification fixture')
    registry=create_core_tool_registry(workspace_root=folder)
    executor=ToolExecutor(store=runtime.store,registry=registry)
    command=f"set -e\nrm -f '{file}'\nfind '{folder}' -maxdepth 1 -type d -name 'cache_*' -delete"
    result=executor.invoke(ToolInvocation('stage-review-'+nonce,'terminal_exec','web:stage-review-'+nonce,{'command':command}))
    assert result.output['reason']=='terminal_destructive_action_confirmation_required',result.output
    aid=result.output['approval_id']
    approval=runtime.store.get_approval_request(aid)
    runtime.store.add_approval_request(replace(approval,context={**approval.context,'workspace_id':workspace}))
    runtime.checkpoint()
    def api(path,method='GET'):
        with urlopen(Request('http://127.0.0.1:8753'+path,method=method),timeout=30) as response:
            return json.load(response)
    samples=[]
    rejected_successfully=False
    try:
        # Refresh, including the maintenance pass that formerly approved deletes.
        for index in range(3):
            started=time.perf_counter();status=api('/api/status?full=1');samples.append(time.perf_counter()-started)
            item=next((a for a in status['approvals'] if a['approval_id']==aid),None)
            assert item is not None,'Stage did not reload the new approval'
            assert item['status']=='pending',item
            assert 'Files to delete (1)' in item['display_detail']
            assert 'Delete matching entries within:' in item['display_detail']
            assert 'exact matches have not been enumerated' in item['display_detail']
            assert file.read_text()=='verification fixture'
        rejected=api('/api/reject/'+aid,method='POST')
        assert rejected['ok'],rejected
        rejected_successfully=True
        stored=bootstrap_persistent_runtime(home/'runtime.db').store.get_approval_request(aid)
        assert stored.status.value=='denied'
        assert file.exists()
        print(json.dumps({'approval_id':aid,'status':'denied','file_preserved':True,'preview':'exact files and search scope separated','status_average_seconds':round(sum(samples)/len(samples),3),'status_max_seconds':round(max(samples),3)}))
    finally:
        # Deny only this synthetic record on a failed check; never approve it.
        try:
            if not rejected_successfully:
                try:
                    api('/api/reject/'+aid,method='POST')
                except (HTTPError,URLError):
                    # A dashboard that has not reloaded cannot deny this fixture.
                    # Clean up only the exact locally-created pending record.
                    fresh=bootstrap_persistent_runtime(home/'runtime.db')
                    current=fresh.store.get_approval_request(aid)
                    if current is not None and current.status.value=='pending':
                        assert current.context.get('workspace_id')==workspace
                        fresh.deny_approval_request(aid,actor='operator',reason='Synthetic verification finished without execution')
        finally:
            file.unlink(missing_ok=True)
            folder.rmdir()


if __name__=='__main__':main()

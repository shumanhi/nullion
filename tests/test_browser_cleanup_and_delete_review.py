from __future__ import annotations

import asyncio
import base64
import errno
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion.approvals import create_permission_grant
from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolExecutor, ToolInvocation, create_core_tool_registry
from nullion.approval_display import approval_display_from_request
from nullion.plugins.browser_plugin.browser_resources import browser_resource_directory, expire_browser_resources
from test_core_tool_permission_matrix import FakeTerminalBackend


def terminal_setup(tmp_path, monkeypatch):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_TERMINAL_ENABLED', '1')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'storage'))
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    backend = FakeTerminalBackend()
    registry = create_core_tool_registry(workspace_root=tmp_path, terminal_executor_backend=backend)
    runtime.active_tool_registry = registry
    runtime.store.add_permission_grant(create_permission_grant(approval_id='general-shell',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission='tool:terminal_exec',
        granted_by='fixture', granted_at=datetime.now(UTC)))
    return runtime, registry, backend


def test_multiline_deletion_preview_separates_search_scope_and_exact_files(tmp_path, monkeypatch):
    runtime, registry, backend = terminal_setup(tmp_path, monkeypatch)
    files = [tmp_path / '画像 一.png', tmp_path / 'archivo.png']
    search = tmp_path / 'scratch'
    search.mkdir()
    command = f"set -e\nrm -f '{files[0]}' '{files[1]}'\nfind '{search}' -maxdepth 1 -type d -name 'job_*' -mtime -1 -exec rm -rf {{}} + 2>/dev/null || true\ndf -h . | tail -1"
    result = ToolExecutor(store=runtime.store, registry=registry).invoke(ToolInvocation('review', 'terminal_exec', 'web:review', {'command': command}))
    approval = runtime.store.get_approval_request(result.output['approval_id'])
    targets = result.output['destructive_targets']
    assert [t['target'] for t in targets] == [str(p.resolve()) for p in [*files, search]]
    assert targets[-1]['target_kind'] == 'search_scope'
    display = approval_display_from_request(approval)
    assert 'Files to delete (2)' in display.detail
    assert 'Delete matching entries within:' in display.detail
    assert 'exact matches have not been enumerated' in display.detail
    assert 'About to delete 3 paths' not in display.detail
    assert not backend.calls
    assert runtime.reconcile_effectively_approved_pending_approvals(actor='status') == []
    assert runtime.store.get_approval_request(approval.approval_id).status.value == 'pending'


def test_multiline_rm_without_find_still_requires_review(tmp_path, monkeypatch):
    runtime, registry, backend = terminal_setup(tmp_path, monkeypatch)
    result = ToolExecutor(store=runtime.store, registry=registry).invoke(ToolInvocation('rm', 'terminal_exec', 'web:review', {'command': f"set -e\nrm -f '{tmp_path / '資料.txt'}'\nprintf done"}))
    assert result.output['reason'] == 'terminal_destructive_action_confirmation_required'
    assert len(result.output['destructive_targets']) == 1
    assert not backend.calls


def test_cleanup_expires_only_owned_inactive_resources(tmp_path):
    root = tmp_path / 'resources'
    unrelated = tmp_path / 'user-files'; unrelated.mkdir(); (unrelated / 'keep').write_text('saved')
    with browser_resource_directory(root, retain=True) as active:
        active_marker = active / '.nullion-browser-resource.json'
        active_marker.write_text(json.dumps({'kind':'browser_resource','version':1,'created_at':time.time()-90000}))
        with browser_resource_directory(root, retain=True) as expired:
            (expired / 'capture.png').write_bytes(b'png')
        (expired / '.nullion-browser-resource.json').write_text(active_marker.read_text())
        foreign = root / 'foreign'; foreign.mkdir(); (foreign / 'keep').touch()
        link = root / 'link'; link.symlink_to(unrelated, target_is_directory=True)
        expire_browser_resources(root)
        assert active.is_dir()
        assert not expired.exists()
        assert (foreign / 'keep').exists() and (unrelated / 'keep').exists() and link.is_symlink()
    expire_browser_resources(root)
    assert not active.exists()


def test_temporary_resource_removed_even_on_exception(tmp_path):
    with pytest.raises(RuntimeError):
        with browser_resource_directory(tmp_path / 'scratch') as folder:
            (folder / 'nested').mkdir()
            raise RuntimeError('cancel')
    assert not folder.exists()


def test_disk_full_stays_typed_through_browser_handler(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter, browser_use_worker as worker
    monkeypatch.setattr(adapter, 'browser_use_python', lambda: sys.executable)
    def full(*args, **kwargs): raise OSError(errno.ENOSPC, 'fixture')
    monkeypatch.setattr(worker, 'run_isolated_browser_task', full)
    result = adapter.browser_task_handler(lambda: object(), adapter.BrowserPolicy())(ToolInvocation('full','browser_run_task','operator',{'task':'比較','url':'https://example.com'}))
    assert result.status == 'failed'
    assert result.output['reason'] == 'storage_exhausted'
    assert result.output['retryable'] is False


def test_source_evidence_expires_but_promoted_delivery_is_durable(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter, browser_use_worker as worker
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'storage'))
    evidence = adapter.PageEvidence('operator', adapter.BrowserPolicy())
    state = SimpleNamespace(url='https://example.com',title='Results',dom_state=None,screenshot=base64.b64encode(b'\x89PNG\r\n\x1a\nfixture').decode())
    asyncio.run(evidence.capture(state))
    result = evidence.result(error=None,elapsed=1,steps=1,done=False)
    source = Path(result['artifact_paths'][0])
    assert 'browser-source-evidence' in str(source)
    monkeypatch.setattr(adapter, 'browser_use_python', lambda: sys.executable)
    monkeypatch.setattr(worker, 'run_isolated_browser_task', lambda *a,**kw: result)
    delivered = adapter.browser_task_handler(lambda: object(),adapter.BrowserPolicy())(ToolInvocation('png','browser_run_task','operator',{'task':'比較','url':'https://example.com'},flow_context={'requested_artifact_extensions':['.png']}))
    durable = Path(delivered.output['artifact_paths'][0])
    root = workspace_storage_roots_for_principal('operator').scratch / 'browser-source-evidence'
    expire_browser_resources(root,now=time.time()+90000)
    assert not source.exists()
    assert durable.read_bytes().startswith(b'\x89PNG')
    assert delivered.output['artifact_descriptors'][0]['role'] == 'deliverable'


@pytest.mark.parametrize('mode',['success','failure','timeout'])
def test_worker_temp_files_are_owned_and_removed_after_process_shutdown(tmp_path,monkeypatch,mode):
    import subprocess
    from nullion.plugins.browser_plugin import browser_use_worker as worker
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_resources.browser_temporary_root', lambda: tmp_path/'sdk-temp')
    program=tmp_path/'fake_browser_sdk.py'
    observed=tmp_path/'observed.json'
    program.write_text('''import json,os,sys,tempfile,time
from pathlib import Path
job=json.loads(Path(sys.argv[1]).read_text())
folder=Path(tempfile.mkdtemp(prefix='browser_use_agent_'))
(folder/'capture').write_bytes(b'bytes')
Path(os.environ['BOUNDARY_OBSERVED']).write_text(json.dumps({'folder':str(folder),'job':sys.argv[1],'pid':os.getpid()}))
if os.environ['BOUNDARY_MODE']=='timeout':time.sleep(60)
if os.environ['BOUNDARY_MODE']=='failure':sys.exit(3)
Path(sys.argv[2]).write_text(json.dumps({'report_status':'blocked','observations':[],'artifact_paths':[]}))
''')
    original=worker.subprocess.Popen
    def launch(argv,**kwargs):
        kwargs['env'].update(BOUNDARY_OBSERVED=str(observed),BOUNDARY_MODE=mode)
        return original([sys.executable,str(program),*argv[2:]],**kwargs)
    monkeypatch.setattr(worker.subprocess,'Popen',launch)
    # Timeout is chosen at the external process boundary; real cleanup runs.
    if mode=='timeout':
        wait=original.wait
        def bounded_wait(process, timeout=None):
            return wait(process,timeout=0.4 if timeout is not None and timeout>20 else timeout)
        monkeypatch.setattr(original,'wait',bounded_wait)
    if mode=='success':
        worker.run_isolated_browser_task(BrowserTask(task='確認',url='https://example.com'),client=object(),principal_id='operator',policy=BrowserPolicy(),timeout=1,python=sys.executable)
    else:
        with pytest.raises((RuntimeError,subprocess.TimeoutExpired)):
            worker.run_isolated_browser_task(BrowserTask(task='確認',url='https://example.com'),client=object(),principal_id='operator',policy=BrowserPolicy(),timeout=1,python=sys.executable)
    state=json.loads(observed.read_text())
    assert Path(state['folder']).is_relative_to(tmp_path/'sdk-temp')
    assert not Path(state['folder']).exists() and not Path(state['job']).exists()
    assert not list((tmp_path/'home'/'browser-task-scratch').glob('run-*'))


def test_general_tool_grants_reconcile_ordinary_approvals_but_not_exact_reviews(tmp_path,monkeypatch):
    from nullion.approvals import create_approval_request
    runtime, _, _ = terminal_setup(tmp_path,monkeypatch)
    ordinary = create_approval_request(requested_by='web:review',action='use_tool',resource='terminal_exec',request_kind='tool',context={'tool_name':'terminal_exec'})
    exact = create_approval_request(requested_by='web:review',action='use_tool',resource='terminal_exec',request_kind='tool',context={'adapter_approval_context':{'tool_permission_scope':'exact_invocation'}})
    for item in [ordinary,exact]:runtime.store.add_approval_request(item)
    reconciled=runtime.reconcile_effectively_approved_pending_approvals()
    assert [a.approval_id for a in reconciled] == [ordinary.approval_id]
    assert runtime.store.get_approval_request(exact.approval_id).status.value == 'pending'


@pytest.mark.parametrize('prefix',["set -e\n", "set -e;\n", "printf ok # comment\n", "set -e\n# comment\n"])
def test_shell_statement_boundaries_do_not_hide_deletes(tmp_path,monkeypatch,prefix):
    runtime,registry,backend=terminal_setup(tmp_path,monkeypatch)
    victim=tmp_path/'archivo#1\n名前.txt'
    command=prefix+f"rm -f '{victim}'"
    result=ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation('quoted','terminal_exec','web:review',{'command':command}))
    assert result.output['reason']=='terminal_destructive_action_confirmation_required'
    assert result.output['destructive_targets'][0]['target']==str(victim)
    assert not backend.calls


@pytest.mark.parametrize('error_number',[errno.ENOSPC,getattr(errno,'EDQUOT',errno.ENOSPC)])
def test_download_preserves_structured_storage_errors(tmp_path,monkeypatch,error_number):
    from nullion.tools import _build_file_download_handler
    class Opener:
        def open(self,*args,**kwargs):raise OSError(error_number,'synthetic storage error')
    monkeypatch.setattr('nullion.tools._build_web_fetch_opener',lambda:Opener())
    result=_build_file_download_handler(workspace_root=tmp_path,include_principal_workspace=False)(ToolInvocation('download','file_download','operator',{'url':'https://93.184.216.34/source','output_path':str(tmp_path/'source.html')}))
    assert result.status=='failed'
    assert result.output['reason']=='storage_exhausted'
    assert result.output['requires_operator_action'] is True


def test_storage_failure_during_resource_creation_leaves_no_orphan(tmp_path,monkeypatch):
    original=Path.write_text
    def full(path,*args,**kwargs):
        if path.name=='.nullion-browser-resource.json':
            raise OSError(errno.ENOSPC,'fixture')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'write_text',full)
    with pytest.raises(OSError):
        with browser_resource_directory(tmp_path/'source',retain=True):
            pytest.fail('Resource creation must fail before running the task')
    assert list((tmp_path/'source').iterdir())==[]


def test_finished_windows_worker_does_not_kill_a_reused_pid(tmp_path,monkeypatch):
    from nullion.plugins.browser_plugin import browser_use_worker as worker
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_resources.browser_temporary_root',lambda:tmp_path/'sdk-temp')
    monkeypatch.setattr(worker.platform,'system',lambda:'Windows')
    def launch(args,**kwargs):
        Path(args[-1]).write_text(json.dumps({'report_status':'blocked','observations':[]}))
        return SimpleNamespace(returncode=0,pid=999,wait=lambda **kwargs:None,poll=lambda:0)
    monkeypatch.setattr(worker.subprocess,'Popen',launch)
    monkeypatch.setattr(worker,'_signal_worker',lambda *args,**kwargs:pytest.fail('An exited Windows PID must not be killed'))
    result=worker.run_isolated_browser_task(BrowserTask(task='確認',url='https://example.com'),client=object(),principal_id='operator',policy=BrowserPolicy(),timeout=1,python=sys.executable)
    assert result['report_status']=='blocked'
    assert not list((tmp_path/'home'/'browser-task-scratch').glob('run-*'))


def test_existing_find_approval_rows_are_shown_as_search_scopes():
    from nullion.approvals import create_approval_request,TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND
    approval=create_approval_request(requested_by='web:review',action='use_tool',resource='terminal_exec',request_kind=TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND,
        context={'command':"find '/workspace/cache' -name '*.tmp' -delete",'destructive_target_count':1,
        'destructive_targets':[{'target':'/workspace/cache','operation':'delete','command_family':'find'}]})
    detail=approval_display_from_request(approval).detail
    assert 'Delete matching entries within:' in detail
    assert 'About to delete 1 path' not in detail
    assert 'exact matches have not been enumerated' in detail

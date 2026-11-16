"""Catalogue integrity without executing or importing any live workflow."""
import builtins
import ast
from copy import deepcopy
import json
import os
import subprocess
import sys
from collections import Counter

import pytest

from scripts import live_stage_test_catalog as catalog


def policy():
    return json.loads((catalog.ROOT/catalog.POLICY).read_text())


def test_complete_inventory_preserves_parameterized_cases_and_execution_types():
    result=catalog.build_catalog()
    summary=result['summary']
    assert summary['expanded_cases']==295
    assert sum(summary['categories'].values())==sum(summary['kinds'].values())==295
    assert summary['kinds']==dict(live_workflow=272,saved_replay=14,
                                 local_harness=9,aggregate_entry_point=0)
    assert len({row['selector'] for row in result['tests']})==len(result['tests'])
    assert summary['api_scenarios']==75 and summary['web_catalogue_scenarios']==48


def test_inventory_does_not_import_app_probes_or_live_test_modules(monkeypatch):
    original=builtins.__import__
    def guarded(name,*args,**kwargs):
        assert not name.startswith(('nullion','tests.e2e','scripts.live_stage_chat_contract_probe')),name
        return original(name,*args,**kwargs)
    monkeypatch.setattr(builtins,'__import__',guarded)
    assert catalog.build_catalog()['summary']['files']==47


@pytest.mark.parametrize('change', ['module','scenario','function','override','run_set','duplicate_category'])
def test_unreviewed_or_stale_catalogue_assignments_are_rejected(change):
    data=deepcopy(policy())
    if change=='module':data['modules'].pop('test_live_stage_transport.py')
    if change=='scenario':data['scenario_categories']['latency'].remove('simple')
    if change=='function':data['reviewed_functions']['test_live_stage_transport.py'].append('test_unreviewed')
    if change=='override':data['test_overrides']['tests/e2e/absent.py::test_absent']={'category':'context'}
    if change=='run_set':data['run_sets']['smoke'].append('tests/e2e/absent.py::test_absent')
    if change=='duplicate_category':data['scenario_categories']['browser'].append('simple')
    with pytest.raises(ValueError):catalog.build_catalog(policy=data)


def test_overlap_audit_retains_distinct_boundaries_and_language_variants():
    result=catalog.build_catalog()
    assert len(result['api_web_overlaps'])==48 and result['workflow_wrappers']==[]
    assert result['identical_test_bodies']==[]
    assert len(result['retired_selectors'])==7
    consent='tests/e2e/test_live_stage_web_extended.py::test_live_web_account_read_waits_for_explicit_source_choice'
    assert {r['selector'] for r in result['tests'] if r['selector'].startswith(consent+'[')}=={
        consent+'[en]',consent+'[es]',consent+'[ja]'}
    variants={'browser_screenshot_artifact_delivery','browser_context_capture_recovery',
              'browser_context_capture_recovery_variant'}
    assert variants <= {row.get('scenario') for row in result['tests']}


def test_all_consolidated_workflows_execute_the_extra_assertions_once():
    requirements=policy()['consolidated_assertions']
    assert len(requirements)==6
    for selector,expected in requirements.items():
        filename,function_name=selector.split('::')
        tree=ast.parse((catalog.ROOT/filename).read_text())
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==function_name)
        calls=[n for n in ast.walk(function) if isinstance(n,ast.Call)
               and isinstance(n.func,ast.Name) and n.func.id=='record_primary_boundary']
        assert len(calls)==1,selector
        keywords={kw.arg:ast.literal_eval(kw.value) for kw in calls[0].keywords}
        assert keywords.get('expected_turns',1)==expected['expected_turns']
        assert keywords.get('allow_deferred',False)==expected['allow_deferred']


def test_category_and_smoke_queries_only_print_valid_reviewed_selectors(capsys):
    catalog.main(['--category','browser','--json'])
    data=json.loads(capsys.readouterr().out)
    assert data['selected_cases']==21
    assert all(row['category']=='browser' for row in data['tests'])
    catalog.main(['--set','smoke','--selectors'])
    selectors=capsys.readouterr().out.splitlines()
    assert len(selectors)==len(set(selectors))==8
    assert selectors==policy()['run_sets']['smoke']
    assert all('::test_' in selector for selector in selectors)


def test_saved_document_matches_reviewed_source_inventory():
    expected=catalog.markdown(catalog.build_catalog(),policy())+'\n'
    assert (catalog.ROOT/'docs/testing/live-stage-test-catalog.md').read_text()==expected


def test_inventory_counts_match_real_pytest_collection_without_running_workflows():
    scenarios,_=catalog.scenario_definitions(catalog.ROOT)
    env={**os.environ,'NULLION_LIVE_STAGE_E2E':'0','NULLION_RUN_E2E':'0',
         'NULLION_LIVE_SCENARIOS':','.join(scenarios)}
    files=[str(path.relative_to(catalog.ROOT)) for path in
           sorted((catalog.ROOT/'tests/e2e').glob('test_live_stage*.py'))]
    result=subprocess.run([sys.executable,'-m','pytest','--collect-only','--verbosity=-1',
                           '-o','addopts=',*files],cwd=catalog.ROOT,env=env,
                          capture_output=True,text=True,timeout=45)
    assert result.returncode==0,result.stdout+result.stderr
    nodes=[line for line in result.stdout.splitlines()
           if line.startswith('tests/e2e/test_live_stage') and '::test_' in line]
    actual=Counter(node.split('[',1)[0] for node in nodes)
    expected=Counter()
    for row in catalog.build_catalog()['tests']:
        expected[row['selector'].split('[',1)[0]]+=row['variants']
    assert actual==expected

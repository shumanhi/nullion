from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_fork_workflow_uses_one_checkout_and_no_repository_secrets():
    workflow = yaml.load((ROOT/'.github/workflows/pr-suite.yml').read_text(), Loader=yaml.BaseLoader)
    assert 'pull_request' in workflow['on']
    assert workflow['permissions'] == {'contents': 'read'}
    for job in workflow['jobs'].values():
        checkouts = [step for step in job['steps'] if step.get('uses', '').startswith('actions/checkout@')]
        assert len(checkouts) == 1
        assert not checkouts[0].get('with', {}).get('repository')
        assert 'secrets.' not in str(job)
    for job_name in ['docker-e2e','linux-installer','windows-installer','macos-installer']:
        assert any('scripts/' in step.get('run','').replace('\\','/') for step in workflow['jobs'][job_name]['steps'])

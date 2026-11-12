from pathlib import Path

import pytest

from scripts.pytest_target_order import order_pytest_selectors


@pytest.mark.parametrize('nested',['統合','integracion'])
def test_explicit_interleaved_files_are_grouped_without_losing_selectors_or_options(tmp_path,nested):
    for relative in ['checks/a.py','checks/b.py',f'checks/{nested}/first.py',f'checks/{nested}/second.py']:
        p=tmp_path/relative;p.parent.mkdir(parents=True,exist_ok=True);p.touch()
    args=['checks/a.py',f'checks/{nested}/first.py::test_value[7]','checks/b.py',
        f'checks/{nested}/second.py','-k','exact expression','--ignore','checks/a.py']
    ordered=order_pytest_selectors(args,cwd=tmp_path)
    assert ordered==['checks/a.py','checks/b.py',f'checks/{nested}/first.py::test_value[7]',
        f'checks/{nested}/second.py','-k','exact expression','--ignore','checks/a.py']
    assert sorted(ordered[:4])==sorted(args[:4])


@pytest.mark.parametrize('args',[
    ['--pyargs','package.tests'],['missing.py','-x'],['-k','checks/a.py','checks/a.py']])
def test_non_path_inputs_and_options_remain_untouched(tmp_path,args):
    assert order_pytest_selectors(args,cwd=tmp_path)==args


def test_directory_selection_and_explicit_duplicates_are_preserved(tmp_path):
    (tmp_path/'checks').mkdir();(tmp_path/'checks/a.py').touch()
    args=['checks/a.py','checks/a.py','checks','--keep-duplicates']
    assert order_pytest_selectors(args,cwd=tmp_path)==args


def test_json_argument_transport_keeps_filters_and_literal_shell_text():
    import json
    from scripts.pytest_target_order import pytest_arguments
    args = ['tests/example.py::case[value with spaces]', '-k', 'alpha or beta',
            '--override-ini', 'addopts=$(touch never-evaluated)']
    assert pytest_arguments(['legacy'], environ={'NULLION_E2E_PYTEST_ARGS_JSON': json.dumps(args)}) == args
    assert pytest_arguments(['legacy'], environ={}) == ['legacy']


def test_docker_launcher_exports_exact_argument_array(tmp_path):
    import json, os, subprocess
    from pathlib import Path
    binary = tmp_path / 'bin'; binary.mkdir()
    captured = tmp_path / 'arguments.json'
    docker = binary / 'docker'
    docker.write_text('#!/usr/bin/env python3\nimport os,sys\nfrom pathlib import Path\n'
        'if "run" in sys.argv: Path(os.environ["CAPTURE_TEST_ARGUMENTS_FILE"]).write_text(os.environ["NULLION_E2E_PYTEST_ARGS_JSON"])\n')
    docker.chmod(0o755)
    args = ['tests/example.py', '-k', 'alpha or beta', '--override-ini', 'addopts=literal $(false)']
    environment = {**os.environ, 'PATH': str(binary) + os.pathsep + os.environ['PATH'],
        'NULLION_E2E_SKIP_BUILD': '1', 'CAPTURE_TEST_ARGUMENTS_FILE': str(captured)}
    result = subprocess.run(['bash', str(Path(__file__).resolve().parents[1] / 'scripts/e2e-docker.sh'), *args],
        env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(captured.read_text()) == args

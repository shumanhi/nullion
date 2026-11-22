"""Real coverage/pytest subprocesses retain failed attempts without weakening gates."""
import json
import os
from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize("case,expected_status", [("pass", 0), ("assertion", 1), ("coverage", 2), ("collection", 2)])
def test_unit_runner_keeps_data_and_preserves_failure_status(tmp_path, case, expected_status):
    source = tmp_path / "src/nullion"
    source.mkdir(parents=True)
    (source / "__init__.py").write_text("")
    (source / "sample.py").write_text("def value():\n    return 37\n" + (
        "\ndef unused():\n" + "".join(f"    item_{i} = {i}\n" for i in range(30))
        if case == "coverage" else ""))
    test = tmp_path / "test_sample.py"
    test.write_text("from nullion.sample import value\ndef test_value():\n    assert value() == "
                    + ("38" if case == "assertion" else "37") + "\n")
    if case == "collection":
        test.write_text("import nonexistent_nullion_coverage_fixture\n")
    runner = Path(__file__).resolve().parents[1] / "scripts/run-unit-coverage.sh"
    env = {**os.environ, "PYTHONPATH": str(tmp_path / "src"),
           "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1", "NULLION_TEST_CANDIDATE_REVISION": "owned-fixture"}
    result = subprocess.run(["bash", str(runner), str(test)], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == expected_status, result.stdout + result.stderr
    attempts = list((tmp_path / ".e2e-artifacts/unit-coverage").iterdir())
    assert len(attempts) == 1
    attempt = attempts[0]
    assert (attempt / ".coverage").stat().st_size > 0
    report = json.loads((attempt / "coverage.json").read_text())
    status = json.loads((attempt / "status.json").read_text())
    assert status["candidate_revision"] == "owned-fixture"
    assert status["tests_exit_code"] == ({"assertion": 1, "collection": 2}.get(case, 0))
    assert status["coverage_exit_code"] == (2 if case in {"coverage", "collection"} else 0)
    assert report["totals"]["percent_covered"] >= 70 if case in {"pass", "assertion"} else report["totals"]["percent_covered"] < 70

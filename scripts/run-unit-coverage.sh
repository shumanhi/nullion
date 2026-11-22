#!/usr/bin/env bash
set -euo pipefail

# Keep each attempt separate, including failed tests. The container exit trap
# exports this directory before Docker removes the disposable workspace.
unit_coverage_id="$(python -c 'from uuid import uuid4; print(uuid4().hex)')"
unit_coverage_dir="$PWD/.e2e-artifacts/unit-coverage/$unit_coverage_id"
mkdir -p "$unit_coverage_dir"
export COVERAGE_FILE="$unit_coverage_dir/.coverage"

unit_test_status=0
python -m coverage run --source=src/nullion -m pytest -q "$@" || unit_test_status=$?
unit_report_status=0
python -m coverage report --fail-under=70 || unit_report_status=$?
unit_json_status=0
python -m coverage json -o "$unit_coverage_dir/coverage.json" || unit_json_status=$?

python - "$unit_coverage_dir/status.json" "$unit_test_status" "$unit_report_status" "$unit_json_status" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

Path(sys.argv[1]).write_text(json.dumps({
    "created_at": datetime.now(timezone.utc).isoformat(),
    "candidate_revision": os.environ.get("NULLION_TEST_CANDIDATE_REVISION"),
    "tests_exit_code": int(sys.argv[2]),
    "coverage_exit_code": int(sys.argv[3]),
    "json_exit_code": int(sys.argv[4]),
    "coverage_data": ".coverage",
    "coverage_report": "coverage.json",
}, indent=2) + "\n")
PY

# Test failure, interruption and collection errors stay authoritative. Coverage
# cannot turn a failed run green; passing tests still require the same 70% gate.
if [[ "$unit_test_status" -ne 0 ]]; then exit "$unit_test_status"; fi
if [[ "$unit_report_status" -ne 0 ]]; then exit "$unit_report_status"; fi
exit "$unit_json_status"

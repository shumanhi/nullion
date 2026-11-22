#!/usr/bin/env bash
# Real-provider stage Web UI acceptance; never points at production.
set -euo pipefail
SUITE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ROOT="${NULLION_APP_REPO:?Set NULLION_APP_REPO to the candidate app checkout}"
STAGE_HOME="${NULLION_LIVE_STAGE_HOME:-${HOME}/.nullion-stage}"
IMAGE="${NULLION_E2E_IMAGE:-nullion-e2e:local}"
OUTPUT="${NULLION_LIVE_EVIDENCE_DIR:-${SUITE_ROOT}/.e2e-artifacts/live-stage-web}"
BROWSER_GUARD="${SUITE_ROOT}/scripts/live_stage_browser_guard.py"
"${STAGE_HOME}/venv/bin/python" "${BROWSER_GUARD}" --stage-home "${STAGE_HOME}" check
BROWSER_SNAPSHOT="$("${STAGE_HOME}/venv/bin/python" "${BROWSER_GUARD}" --stage-home "${STAGE_HOME}" snapshot)"
CONTAINER="nullion-live-web-$(date +%s)-$$"
[[ -f "${STAGE_HOME}/runtime.db" && -f "${APP_ROOT}/pyproject.toml" ]]
[[ "${NULLION_LIVE_STAGE_E2E:-}" == "1" ]] || { echo 'Set NULLION_LIVE_STAGE_E2E=1 to run live stage requests.' >&2; exit 2; }
[[ "${STAGE_HOME}" == "${HOME}/.nullion-stage" ]] || { echo 'Native evidence reader requires the standard stage home.' >&2; exit 2; }
mkdir -p "${OUTPUT}"
TIMING_HISTORY="${SUITE_ROOT}/.e2e-artifacts/test-performance"
mkdir -p "${TIMING_HISTORY}"
TEST_REVISION="$(git -C "${SUITE_ROOT}" rev-parse HEAD 2>/dev/null || true)"
CANDIDATE_REVISION="$(git -C "${APP_ROOT}" rev-parse HEAD 2>/dev/null || true)"
TEST_DIRTY=0
[[ -z "$(git -C "${SUITE_ROOT}" status --porcelain 2>/dev/null)" ]] || TEST_DIRTY=1
EVIDENCE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nullion-stage-evidence.XXXXXX")"
EVIDENCE_PID=""
# Share only an owned ephemeral fixture directory with the running stage
# workspace. Container-local files are invisible to the stage file tools.
FIXTURE_ROOT="$(mktemp -d "${STAGE_HOME}/workspaces/workspace_admin/artifacts/live-contract.XXXXXX")"
cleanup() {
  local task_exit_code=$?
  docker cp "${CONTAINER}:/tmp/live-web-evidence/." "${OUTPUT}/" 2>/dev/null || true
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
  if [[ -n "${EVIDENCE_PID}" ]]; then
    kill "${EVIDENCE_PID}" 2>/dev/null || true
    wait "${EVIDENCE_PID}" 2>/dev/null || true
  fi
  rm -f "${EVIDENCE_DIR}/connection.json"
  rmdir "${EVIDENCE_DIR}" 2>/dev/null || true
  rm -rf "${FIXTURE_ROOT}"
  if ! "${STAGE_HOME}/venv/bin/python" "${BROWSER_GUARD}" --stage-home "${STAGE_HOME}" \
      --expected-snapshot "${BROWSER_SNAPSHOT}" verify-unchanged; then
    task_exit_code=1
  fi
  trap - EXIT
  exit "${task_exit_code}"
}
trap cleanup EXIT
docker run -d --name "${CONTAINER}" \
  -v "${TIMING_HISTORY}:/test-performance:rw" \
  -e NULLION_TEST_PERFORMANCE_DIR=/test-performance \
  -e NULLION_TEST_REVISION="${TEST_REVISION}" \
  -e NULLION_TEST_CANDIDATE_REVISION="${CANDIDATE_REVISION}" \
  -e NULLION_TEST_DIRTY="${TEST_DIRTY}" \
  -e NULLION_TEST_PROVIDER="${NULLION_TEST_PROVIDER:-}" \
  -e NULLION_TEST_MODEL="${NULLION_TEST_MODEL:-}" \
  -e NULLION_E2E_IMAGE="${IMAGE}" \
  -e NULLION_LIVE_STAGE_ARTIFACT_ROOT="${FIXTURE_ROOT}" \
  -e NULLION_LIVE_STAGE_UPLOAD_ROOT="${FIXTURE_ROOT}" \
  -v "${FIXTURE_ROOT}:${FIXTURE_ROOT}:rw" \
  -e NULLION_LIVE_REPEAT="${NULLION_LIVE_REPEAT:-1}" \
  -e NULLION_LIVE_VERBOSE_SOURCE_FILE="${NULLION_LIVE_VERBOSE_SOURCE_FILE:-}" \
  -e NULLION_LIVE_FIXTURE_PORTS="${NULLION_LIVE_FIXTURE_PORTS:-[]}" \
  -e NULLION_LIVE_RESERVATION_PROMPT="${NULLION_LIVE_RESERVATION_PROMPT:-}" \
  -e NULLION_LIVE_SCENARIOS="${NULLION_LIVE_SCENARIOS:-}" \
  -e NULLION_LIVE_TRIP_REPLAY_PROMPT="${NULLION_LIVE_TRIP_REPLAY_PROMPT:-}" \
  -e NULLION_LIVE_PROFILE_CITY_NAMES="${NULLION_LIVE_PROFILE_CITY_NAMES:-}" \
  -e NULLION_LIVE_EMAIL_STATUS_PROMPT="${NULLION_LIVE_EMAIL_STATUS_PROMPT:-}" \
  -e NULLION_LIVE_IMAGE_REJECTION_STATUS="${NULLION_LIVE_IMAGE_REJECTION_STATUS:-}" \
  -e NULLION_LIVE_SOURCE_COMPARISON_PROMPT="${NULLION_LIVE_SOURCE_COMPARISON_PROMPT:-}" \
  -e NULLION_LIVE_SOURCE_EXPECTED_VALUES="${NULLION_LIVE_SOURCE_EXPECTED_VALUES:-}" \
  -e NULLION_LIVE_SOURCE_EXPECTED_BROWSER_CALLS="${NULLION_LIVE_SOURCE_EXPECTED_BROWSER_CALLS:-}" \
  -e NULLION_LIVE_COVERAGE_CASE="${NULLION_LIVE_COVERAGE_CASE:-}" \
  -e NULLION_LIVE_RECEIPT_REPLAY_ID="${NULLION_LIVE_RECEIPT_REPLAY_ID:-}" \
  -e NULLION_LIVE_REVIEW_LAYOUT_REPLAY="${NULLION_LIVE_REVIEW_LAYOUT_REPLAY:-}" \
  -e NULLION_LIVE_SOURCE_NOTE="${NULLION_LIVE_SOURCE_NOTE:-}" \
  -e NULLION_LIVE_CAPTURE_SOURCE="${NULLION_LIVE_CAPTURE_SOURCE:-}" \
  -e NULLION_LIVE_CAPTURE_REPLAY="${NULLION_LIVE_CAPTURE_REPLAY:-}" \
  -e NULLION_LIVE_PDF_REPLAY="${NULLION_LIVE_PDF_REPLAY:-}" \
  -e NULLION_LIVE_CODEMODE_RECALL_SOURCE="${NULLION_LIVE_CODEMODE_RECALL_SOURCE:-}" \
  -e NULLION_LIVE_BOUNDED_COMPARISON_SOURCE="${NULLION_LIVE_BOUNDED_COMPARISON_SOURCE:-}" \
  -e NULLION_LIVE_FALLBACK_SOURCE_URL="${NULLION_LIVE_FALLBACK_SOURCE_URL:-}" \
  -e NULLION_LIVE_FALLBACK_EXPECTED_TEXT="${NULLION_LIVE_FALLBACK_EXPECTED_TEXT:-}" \
  -e NULLION_INCIDENT_IMAGE="${NULLION_INCIDENT_IMAGE:-}" \
  -e NULLION_STAGE_CRON_ID="${NULLION_STAGE_CRON_ID:-}" \
  -e NULLION_STAGE_CRON_EXPECTED_WRITE="${NULLION_STAGE_CRON_EXPECTED_WRITE:-}" \
  -e NULLION_STAGE_CRON_EXPECTED_MODE="${NULLION_STAGE_CRON_EXPECTED_MODE:-}" \
  -e NULLION_LIVE_NEWS_SOURCE="${NULLION_LIVE_NEWS_SOURCE:-}" \
  -e NULLION_LIVE_OBSERVATION_CDP_URL="${NULLION_LIVE_OBSERVATION_CDP_URL:-}" \
  -e NULLION_LIVE_SNAPSHOT_REPLAY_NAME="${NULLION_LIVE_SNAPSHOT_REPLAY_NAME:-}" \
  -e NULLION_LIVE_SNAPSHOT_PUBLIC_SOURCE="${NULLION_LIVE_SNAPSHOT_PUBLIC_SOURCE:-}" \
  -e NULLION_LIVE_SNAPSHOT_READY_SELECTOR="${NULLION_LIVE_SNAPSHOT_READY_SELECTOR:-}" \
  -e NULLION_LIVE_CRON_REPLAY_FILE="${NULLION_LIVE_CRON_REPLAY_FILE:-}" \
  -e NULLION_LIVE_REPORT_REPLAY_FILE="${NULLION_LIVE_REPORT_REPLAY_FILE:-}" \
  -v "${APP_ROOT}:/candidate:ro" -v "${SUITE_ROOT}:/suite:ro" \
  -v "${STAGE_HOME}:/stage:ro" --entrypoint sleep "${IMAGE}" infinity >/dev/null
# Native read-only SQLite avoids Docker Desktop shared-file page races. The
# private, ephemeral bridge serves only this probe's conversation prefixes.
python3 "${SUITE_ROOT}/scripts/live_stage_evidence_server.py"   --bind 0.0.0.0 --connection-file "${EVIDENCE_DIR}/connection.json" &
EVIDENCE_PID=$!
for _ in {1..100}; do
  [[ -s "${EVIDENCE_DIR}/connection.json" ]] && break
  kill -0 "${EVIDENCE_PID}" 2>/dev/null || { echo 'Native stage evidence reader failed.' >&2; exit 1; }
  sleep .05
done
[[ -s "${EVIDENCE_DIR}/connection.json" ]]
docker cp "${EVIDENCE_DIR}/connection.json" "${CONTAINER}:/tmp/native-stage-evidence.json"
docker exec "${CONTAINER}" bash -e -o pipefail -c 'mkdir -p /tmp/live-candidate; tar -C /candidate --exclude="./.e2e-artifacts" -cf - . | tar -C /tmp/live-candidate -xf -; rm -rf /tmp/live-candidate/.e2e-artifacts; ln -s /candidate/.e2e-artifacts /tmp/live-candidate/.e2e-artifacts; rm -rf /tmp/live-candidate/tests; mkdir -p /tmp/live-candidate/tests /tmp/live-candidate/scripts; cp -a /suite/tests/. /tmp/live-candidate/tests/; cp -a /suite/scripts/. /tmp/live-candidate/scripts/; cp /suite/conftest.py /tmp/live-candidate/conftest.py; cd /tmp/live-candidate; python -m pip install -e ".[test]" --no-build-isolation'
docker exec -i "${CONTAINER}" sh -c 'cat > /tmp/stage_web_runner.py' <<'PY'
import asyncio
import os
import subprocess
import sys
import threading

sys.path.insert(0, '/tmp/live-candidate/scripts')
from live_stage_tcp_forwarder import forward_connection,live_forward_ports

ready = threading.Event()
async def forward():
    servers=[]
    for port in live_forward_ports(os.environ.get('NULLION_LIVE_FIXTURE_PORTS','[]')):
        async def connect(reader,writer,selected_port=port):
            await forward_connection(reader,writer,host='host.docker.internal',port=selected_port)
        servers.append(await asyncio.start_server(connect,'127.0.0.1',port))
    ready.set()
    await asyncio.gather(*(server.serve_forever() for server in servers))
threading.Thread(target=lambda: asyncio.run(forward()), daemon=True).start()
assert ready.wait(10), 'Stage-only forwarder did not start'
os.environ.update(NULLION_APP_REPO='/tmp/live-candidate', NULLION_RUN_E2E='1',
                  NULLION_E2E_CONTAINER='1', NULLION_LIVE_STAGE_E2E='1',
                  NULLION_LIVE_BASE_URL='http://127.0.0.1:8753',
                  NULLION_LIVE_RUNTIME_DB='/stage/runtime.db',
                  NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION='/tmp/native-stage-evidence.json',
                  NULLION_LIVE_EVIDENCE_DIR='/tmp/live-web-evidence')
os.chdir('/tmp/live-candidate')
os.makedirs('/tmp/live-web-evidence', exist_ok=True)
tests = sys.argv[1:] or ['tests/e2e/test_live_stage_web_matrix.py',
                         'tests/e2e/test_live_stage_web_followups.py',
                         'tests/e2e/test_live_stage_web_extended.py',
                         'tests/e2e/test_live_stage_live_tracking.py']
raise SystemExit(subprocess.call([sys.executable, '-m', 'pytest', *tests, '-vv', '--tb=short',
                                 '--junitxml=/tmp/live-web-evidence/results.xml']))
PY
docker exec "${CONTAINER}" python /tmp/stage_web_runner.py "$@"

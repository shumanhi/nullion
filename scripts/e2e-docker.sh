#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
if [[ $# -gt 0 ]]; then
  export NULLION_E2E_PYTEST_ARGS="$*"
  export NULLION_E2E_PYTEST_ARGS_JSON="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' "$@")"
fi
export NULLION_E2E_PYTEST_ARGS="${NULLION_E2E_PYTEST_ARGS:-tests/e2e}"
export NULLION_RUN_E2E="${NULLION_RUN_E2E:-1}"
export NULLION_APP_REPO_HOST="$(cd "${NULLION_APP_REPO:-$ROOT_DIR}" && pwd)"
export NULLION_TEST_REVISION="${NULLION_TEST_REVISION:-$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || true)}"
export NULLION_TEST_CANDIDATE_REVISION="${NULLION_TEST_CANDIDATE_REVISION:-$(git -C "$NULLION_APP_REPO_HOST" rev-parse HEAD 2>/dev/null || true)}"
if [[ -z "${NULLION_TEST_DIRTY+x}" ]]; then
  export NULLION_TEST_DIRTY=0
  [[ -z "$(git -C "$ROOT_DIR" status --porcelain 2>/dev/null)" ]] || export NULLION_TEST_DIRTY=1
fi
mkdir -p .e2e-artifacts
export NULLION_E2E_OUTPUT_UID="$(id -u)"
export NULLION_E2E_OUTPUT_GID="$(id -g)"
if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
else
  compose=(docker-compose)
fi
if [[ "${NULLION_E2E_SKIP_BUILD:-0}" != 1 ]]; then
  "${compose[@]}" -f docker-compose.e2e.yml build nullion-e2e
fi
"${compose[@]}" -f docker-compose.e2e.yml run --rm nullion-e2e

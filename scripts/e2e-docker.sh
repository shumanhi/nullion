#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
if [[ $# -gt 0 ]]; then
  export NULLION_E2E_PYTEST_ARGS="$*"
fi
export NULLION_E2E_PYTEST_ARGS="${NULLION_E2E_PYTEST_ARGS:-tests/e2e}"
export NULLION_RUN_E2E="${NULLION_RUN_E2E:-1}"
export NULLION_APP_REPO_HOST="$(cd "${NULLION_APP_REPO:-$ROOT_DIR}" && pwd)"
mkdir -p .e2e-artifacts
if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
else
  compose=(docker-compose)
fi
if [[ "${NULLION_E2E_SKIP_BUILD:-0}" != 1 ]]; then
  "${compose[@]}" -f docker-compose.e2e.yml build nullion-e2e
fi
"${compose[@]}" -f docker-compose.e2e.yml run --rm nullion-e2e

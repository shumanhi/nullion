#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export NULLION_RUN_E2E=0
export NULLION_TEST_COVERAGE=1
if [[ $# -eq 0 ]]; then
  set -- tests --ignore=tests/e2e
fi
exec "$ROOT_DIR/scripts/e2e-docker.sh" "$@"

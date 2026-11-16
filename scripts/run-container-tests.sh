#!/usr/bin/env bash
set -euo pipefail
# Both source mounts are read-only. All installs, runtime data and test output
# live in the container; the contributor checkout and host services stay intact.
mkdir -p /tmp/nullion-src
# Saved evidence is not source. This runner creates fresh writable outputs
# below and timing history has its own mount; do not copy history first.
tar -C /inputs/app --exclude='./.e2e-artifacts' -cf - . | tar -C /tmp/nullion-src -xf -
rm -rf /tmp/nullion-src/.git
# The suite owns collection. An older candidate must not resurrect retired tests.
rm -rf /tmp/nullion-src/tests
mkdir -p /tmp/nullion-src/{tests,scripts,docker}
cp -a /inputs/suite/tests/. /tmp/nullion-src/tests/
cp -a /inputs/suite/scripts/. /tmp/nullion-src/scripts/
cp -a /inputs/suite/docker/. /tmp/nullion-src/docker/
cp /inputs/suite/conftest.py /inputs/suite/docker-compose.e2e.yml /tmp/nullion-src/
cd /tmp/nullion-src
rm -rf .e2e-artifacts
export_results() {
  if [[ -d /outputs ]]; then
    [[ ! -d .e2e-artifacts ]] || cp -a .e2e-artifacts/. /outputs/
    [[ ! -f /tmp/nullion-e2e-coverage.json ]] || cp /tmp/nullion-e2e-coverage.json /outputs/
    # Keep private evidence private, but owned by the invoking host user so
    # Linux CI can upload it after the root container exits.
    if [[ $(id -u) == 0 && ${NULLION_E2E_OUTPUT_UID:-} =~ ^[0-9]+$ && ${NULLION_E2E_OUTPUT_GID:-} =~ ^[0-9]+$ ]]; then
      chown -hR "$NULLION_E2E_OUTPUT_UID:$NULLION_E2E_OUTPUT_GID" /outputs
    fi
  fi
}
trap export_results EXIT
export NULLION_APP_REPO="$PWD"
python -m pip install --disable-pip-version-check --no-build-isolation '.[test]'
# Legacy environment selectors remain whitespace-separated. CLI selectors use
# the JSON array decoded by pytest_target_order so option values stay intact.
read -r -a pytest_args <<< "${NULLION_E2E_PYTEST_ARGS:-tests/e2e}"
# Group explicit parent/child paths so pytest reuses the same fixture collectors.
pytest_args_file="$(mktemp)"
python scripts/pytest_target_order.py "${pytest_args[@]}" > "$pytest_args_file"
mapfile -d '' -t pytest_args < "$pytest_args_file"
rm -f "$pytest_args_file"
if [[ ${#pytest_args[@]} -eq 0 ]]; then
  echo "No test selectors remained after argument normalization." >&2
  exit 2
fi
if [[ "${NULLION_TEST_COVERAGE:-0}" == 1 ]]; then
  python -m coverage run --source=src/nullion -m pytest -q "${pytest_args[@]}"
  python -m coverage report --fail-under=70
else
  if [[ "${NULLION_E2E_BATCH_FILES:-0}" -gt 0 ]]; then
    export NULLION_E2E_BATCH_EVIDENCE=/outputs/batches
    python scripts/run_e2e_batches.py --batch-files "$NULLION_E2E_BATCH_FILES" -- "${pytest_args[@]}"
  else
    python -m pytest -q "${pytest_args[@]}"
  fi
fi
